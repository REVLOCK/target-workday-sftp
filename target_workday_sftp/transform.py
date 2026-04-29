"""Journal CSV to Workday journal CSV (same flow as Oracle ``transform_csv``)."""

from __future__ import annotations

import csv
import logging
import math
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping

from target_workday_sftp.const import (
    INPUT_FILENAME,
    REQUIRED_INPUT_COLUMNS,
    TRANSFORM_OUTPUT_DEFAULT_DATE_STRFTIME,
    TRANSFORM_OUTPUT_DIR_DEFAULT,
    WORKDAY_OUTPUT_COLUMNS,
)
from target_workday_sftp.exceptions import InputError, TransformError, ValidationError

logger = logging.getLogger(__name__)


def _str_from_config(config: Mapping[str, Any], key: str) -> str:
    """Stripped config value or empty (Oracle ``_str_from_config``)."""
    v = config.get(key)
    if v is None:
        return ""
    return str(v).strip()


def _safe_str(value: Any, default: str = "") -> str:
    """Strip to string; None/empty/NaN → default (Oracle ``_safe_str``)."""
    if value is None or value == "":
        return default
    s = str(value).strip()
    if s.lower() in ("nan", "none"):
        return default
    return s


def _is_na_like(val: Any) -> bool:
    """True for missing / blank / NaN-like cell values."""
    if val is None:
        return True
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return True
    s = str(val).strip()
    if not s:
        return True
    if s.lower() in ("nan", "none", "null"):
        return True
    if s in (r"\N", "\\N"):
        return True
    return False


def _validate_row(row: dict[str, Any], row_num: int, je_id: str) -> tuple[list[str], list[str]]:
    """Return (errors, warnings) for one row (same checks as Oracle ``_validate_row``)."""
    errors: list[str] = []
    warnings: list[str] = []

    acct = row.get("Account Number")
    if acct is None or (isinstance(acct, str) and not acct.strip()):
        errors.append(
            f"Row {row_num}: Account Number is required (Journal Entry: {je_id})"
        )

    posting_type = _safe_str(row.get("Posting Type", "")).upper()
    if posting_type and posting_type not in ("DEBIT", "CREDIT"):
        errors.append(
            f"Row {row_num}: Posting Type must be Debit or Credit, got '{posting_type}' "
            f"(Journal Entry: {je_id})"
        )

    amount = row.get("Amount")
    try:
        if amount not in (None, ""):
            float(amount)
    except (ValueError, TypeError):
        errors.append(
            f"Row {row_num}: Invalid Amount '{amount}' (Journal Entry: {je_id})"
        )

    tx_date = row.get("Transaction Date")
    if tx_date and tx_date not in (None, ""):
        try:
            datetime.strptime(str(tx_date).strip(), "%Y-%m-%d")
        except (ValueError, TypeError):
            warnings.append(
                f"Row {row_num}: Transaction Date '{tx_date}' may not parse correctly "
                f"(expected YYYY-MM-DD)"
            )

    return errors, warnings


def _blank_str(val: Any) -> str:
    if _is_na_like(val):
        return ""
    s = str(val).strip()
    if s.lower() == "nan" or s in (r"\N", "\\N"):
        return ""
    return s


def _line_memo(row: Mapping[str, Any], config: Mapping[str, Any]) -> str:
    jememo = _str_from_config(config, "JournalEntryMemo")
    ptype = _blank_str(row.get("Product Type", ""))
    pcode = _blank_str(row.get("Product Code", ""))
    return f"{jememo} {ptype} {pcode}"


def _revenue_category(row: Mapping[str, Any]) -> str:
    code = _blank_str(row.get("Product Code", ""))
    ptype = _blank_str(row.get("Product Type", ""))
    if code and ptype:
        return f"{code} - {ptype}"
    return code or ptype


def _build_empty_workday_row() -> Dict[str, str]:
    """Empty output row template."""
    return dict.fromkeys(WORKDAY_OUTPUT_COLUMNS, "")


# Output columns not set by the shared ``_str_from_config`` loop below.
_TRANSFORM_ROW_SKIP_STR_FROM_CONFIG = frozenset(
    {
        "Currency",
        "AccountingDate",
        "JournalLineOrder",
        "LineCompanyReferenceID",
        "LedgerAccountReferenceID",
        "LineMemo",
        "DebitAmount",
        "CreditAmount",
        "LineCurrency",
        "LedgerDebitAmount",
        "LedgerCreditAmount",
        "Worktag_Revenue_Category_ID",
        "Worktag_Sales_Item_ID",
    }
)


def transform_row(
    row: Mapping[str, Any],
    config: Mapping[str, Any],
    line_order: int,
) -> Dict[str, str]:
    """Map one input CSV row to Workday journal columns (Oracle ``transform_row`` analogue)."""
    out = _build_empty_workday_row()

    type_raw = row.get("Posting Type", "")
    et_raw = str(type_raw).strip().casefold()
    if et_raw not in ("debit", "credit"):
        raise TransformError(f"Type must be Debit or Credit, got {type_raw!r}")

    # Amount strings (Oracle ``transform_row``: ``float(row.get("Amount") or 0)``, ``str(round(..., 2))``).
    try:
        amount_val = float(row.get("Amount") or 0)
    except (ValueError, TypeError):
        amount_val = 0.0
    amount_str = str(round(amount_val, 2))
    debit = amount_str if et_raw == "debit" else ""
    credit = amount_str if et_raw == "credit" else ""

    line_company = _blank_str(row.get("MarketID Finance", ""))
    ledger_id = _blank_str(row.get("Account Number", ""))
    cur = _blank_str(row.get("Currency", ""))
    rev = _revenue_category(row)
    sales_item = _blank_str(row.get("Product Type", ""))
    line_memo = _line_memo(row, config)

    # Every ``_str_from_config`` column in ``WORKDAY_OUTPUT_COLUMNS`` order (JournalKey … tail externals).
    for col in WORKDAY_OUTPUT_COLUMNS:
        if col in _TRANSFORM_ROW_SKIP_STR_FROM_CONFIG:
            continue
        out[col] = _str_from_config(config, col)

    out["Currency"] = cur
    out["AccountingDate"] = _blank_str(row.get("Transaction Date", ""))
    out["JournalLineOrder"] = str(line_order)
    out["LineCompanyReferenceID"] = line_company
    out["LedgerAccountReferenceID"] = ledger_id
    out["LineMemo"] = line_memo
    out["DebitAmount"] = debit
    out["CreditAmount"] = credit
    out["LineCurrency"] = cur
    out["LedgerDebitAmount"] = debit
    out["LedgerCreditAmount"] = credit
    out["Worktag_Revenue_Category_ID"] = rev
    out["Worktag_Sales_Item_ID"] = sales_item

    return out


def _transform_output_csv_path(config: Mapping[str, Any]) -> Path:
    """Destination path: ``{UTC_YYYYMMDD}.csv`` under configured or default output dir."""
    name = (
        datetime.now(timezone.utc).strftime(TRANSFORM_OUTPUT_DEFAULT_DATE_STRFTIME)
        + ".csv"
    )
    out_dir_raw = config.get("transform_output_dir")
    if out_dir_raw not in (None, ""):
        out_dir = Path(str(out_dir_raw)).expanduser().resolve()
    else:
        out_dir = Path(TRANSFORM_OUTPUT_DIR_DEFAULT).resolve()
    return out_dir / name


def transform_journal_summary(config: Dict[str, Any]) -> Path:
    """Read ``input_path``/``JournalEntries.csv``, write Workday CSV (Oracle ``transform_csv`` pattern)."""
    root = Path(config["input_path"]).expanduser().resolve()
    input_file = root / INPUT_FILENAME

    if not input_file.exists():
        raise InputError(f"Input file not found: {input_file}")

    logger.info("Reading journal summary path=%s", input_file)
    with open(input_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        if not rows:
            raise InputError("Input CSV has no data rows")
        cols = list(rows[0].keys())

    missing = [c for c in REQUIRED_INPUT_COLUMNS if c not in cols]
    if missing:
        raise InputError(f"Input CSV missing required columns: {missing}")

    logger.info("Loaded journal summary rows=%s columns=%s", len(rows), len(cols))

    out_path = _transform_output_csv_path(config)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fd, tmp_name = tempfile.mkstemp(
        suffix=".csv",
        prefix=".workday_journal_",
        dir=str(out_path.parent),
    )
    os.close(fd)
    tmp_path = Path(tmp_name)
    wrote_output = False
    success_count = 0
    collected_warnings: list[dict[str, Any]] = []
    try:
        with open(tmp_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=list(WORKDAY_OUTPUT_COLUMNS),
                delimiter=",",
                extrasaction="ignore",
            )
            writer.writeheader()

            for row_num, row in enumerate(rows, start=2):
                je_id = _safe_str(row.get("Journal Entry Id", ""))
                errors, warnings = _validate_row(dict(row), row_num, je_id)

                for w in warnings:
                    collected_warnings.append(
                        {
                            "row": row_num,
                            "journal_entry_id": je_id,
                            "message": w,
                        }
                    )
                    logger.warning(w)

                if errors:
                    err_payload = [
                        {
                            "row": row_num,
                            "journal_entry_id": je_id,
                            "message": e,
                        }
                        for e in errors
                    ]
                    raise ValidationError(
                        f"Validation failed at row {row_num}: {errors[0]}",
                        response={
                            "errors": err_payload,
                            "warnings": collected_warnings,
                        },
                    )

                line_order = row_num - 1
                try:
                    out_row = transform_row(row, config, line_order)
                    writer.writerow(out_row)
                    success_count += 1
                except Exception as e:
                    err_msg = f"Row {row_num}: Transform failed - {e}"
                    logger.exception(err_msg)
                    raise TransformError(err_msg, response=e) from e

        try:
            if out_path.exists():
                out_path.unlink()
        except OSError as exc:
            logger.warning("Could not remove prior output file %s: %s", out_path, exc)
        os.replace(str(tmp_path), str(out_path))
        wrote_output = True

        logger.info(
            "Transform rows=%d ok=%d fail=%d warn=%d → %s",
            len(rows),
            success_count,
            len(rows) - success_count,
            len(collected_warnings),
            out_path,
        )
        return out_path
    finally:
        if not wrote_output and tmp_path.exists():
            try:
                tmp_path.unlink()
                logger.debug("Removed temp file: %s", tmp_path)
            except OSError as exc:
                logger.warning("Could not remove temp %s: %s", tmp_path, exc)


__all__ = ["transform_journal_summary", "transform_row"]
