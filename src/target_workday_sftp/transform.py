"""CSV transform: journal layout or passthrough."""

from __future__ import annotations

import calendar
import json
import logging
import re
import tempfile
from dataclasses import dataclass
from json import JSONDecodeError
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple, cast

import pandas as pd

from target_workday_sftp.const import (
    WORKDAY_DEFAULT_ADJUSTMENT_JOURNAL,
    WORKDAY_DEFAULT_CURRENCY_RATE_TYPE,
    WORKDAY_DEFAULT_EXCLUDE_FROM_SPEND,
    WORKDAY_DEFAULT_LINE_CURRENCY_RATE,
    WORKDAY_DEFAULT_SUBMIT,
    WORKDAY_OUTPUT_COLUMNS,
)
from target_workday_sftp.exceptions import TransformError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkdayJournalBuildConfig:
    """Journal field defaults from config."""

    journal_key: str
    journal_entry_memo: str
    journal_source: str
    journal_line_memo_prefix: str
    company_reference_id_type: str
    company_reference_id: str
    ledger_type: str
    line_company_reference_id_type: str
    ledger_account_parent_id_type: str
    ledger_account_parent_id: str
    ledger_account_reference_id_type: str
    worktag_cost_center_id: str
    worktag_cost_center_pattern: Optional[str]
    accounting_date_closing_day: int

    @classmethod
    def from_target_config(cls, config: Mapping[str, Any]) -> WorkdayJournalBuildConfig:
        """Build from flat target config."""
        journal_memo = str(config.get("journal_entry_memo") or "Chargebee RevRec")
        journal_source = str(config.get("journal_source") or journal_memo)
        cc_pat = config.get("worktag_cost_center_pattern")
        closing_raw = config.get("accounting_date_closing_day")
        if closing_raw in (None, ""):
            closing_day = 15
        else:
            try:
                closing_day = int(closing_raw)
            except (TypeError, ValueError) as exc:
                raise TransformError(
                    f"accounting_date_closing_day must be an integer, got {closing_raw!r}"
                ) from exc
        if not 1 <= closing_day <= 31:
            raise TransformError(
                f"accounting_date_closing_day must be 1-31, got {closing_day}"
            )

        return cls(
            journal_key=str(config.get("journal_key") or "1"),
            journal_entry_memo=journal_memo,
            journal_source=journal_source,
            journal_line_memo_prefix=str(
                config.get("journal_line_memo_prefix") or journal_memo
            ),
            company_reference_id_type=str(
                config.get("company_reference_id_type") or "Company_Reference_ID"
            ),
            company_reference_id=str(config.get("company_reference_id") or "CIRC"),
            ledger_type=str(config.get("ledger_type") or "ACTUALS"),
            line_company_reference_id_type=str(
                config.get("line_company_reference_id_type") or "Company_Reference_ID"
            ),
            ledger_account_parent_id_type=str(
                config.get("ledger_account_parent_id_type") or "Account_Set_ID"
            ),
            ledger_account_parent_id=str(
                config.get("ledger_account_parent_id") or "Child"
            ),
            ledger_account_reference_id_type=str(
                config.get("ledger_account_reference_id_type") or "Ledger_Account_ID"
            ),
            worktag_cost_center_id=str(config.get("worktag_cost_center_id") or "400"),
            worktag_cost_center_pattern=(
                str(cc_pat) if cc_pat not in (None, "") else None
            ),
            accounting_date_closing_day=closing_day,
        )


def _coerce_column_map(raw: Any) -> Dict[str, str]:
    """column_map as dict or JSON object string."""
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return {str(k): str(v) for k, v in raw.items()}
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except JSONDecodeError as exc:
            raise TransformError(f"column_map is not valid JSON: {exc}") from exc
        if not isinstance(parsed, dict):
            raise TransformError("column_map JSON must be an object")
        return {str(k): str(v) for k, v in parsed.items()}
    raise TransformError(f"column_map must be object or JSON string, got {type(raw)!r}")


def _coerce_columns_order(raw: Any) -> Optional[List[str]]:
    """columns_order as list or JSON array string."""
    if raw is None:
        return None
    if isinstance(raw, list):
        return [str(c) for c in raw]
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except JSONDecodeError as exc:
            raise TransformError(f"columns_order is not valid JSON: {exc}") from exc
        if not isinstance(parsed, list):
            raise TransformError("columns_order JSON must be a list of column names")
        return [str(c) for c in parsed]
    raise TransformError(f"columns_order must be list or JSON string, got {type(raw)!r}")


def _norm_header(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(name).strip().lower()).strip("_")


def _find_column(df: pd.DataFrame, *candidates: str) -> Optional[str]:
    cand_keys = {_norm_header(c) for c in candidates}
    for col in df.columns:
        if _norm_header(col) in cand_keys:
            return str(col)
    return None


def _require_column(df: pd.DataFrame, *candidates: str) -> str:
    col = _find_column(df, *candidates)
    if not col:
        raise TransformError(
            f"Missing required column matching one of {candidates!r}; "
            f"have columns={list(df.columns)}"
        )
    return col


def _actg_period_to_accounting_date(period: Any, closing_day: int) -> str:
    """Accounting period string to YYYY-MM-DD."""
    if period is None or (isinstance(period, float) and pd.isna(period)):
        raise TransformError("Actg Period is empty")
    p = str(period).strip()
    if re.fullmatch(r"\d{6}", p):
        year, month = int(p[:4]), int(p[4:6])
    elif re.fullmatch(r"\d{4}-\d{2}", p):
        year, month = int(p[:4]), int(p[5:7])
    else:
        raise TransformError(
            f"Unsupported Actg Period format {period!r}; expected YYYYMM or YYYY-MM"
        )
    if not 1 <= month <= 12:
        raise TransformError(f"Invalid month in Actg Period {period!r}")
    if not 1 <= closing_day <= 31:
        raise TransformError("accounting_date_closing_day must be between 1 and 31")
    last_day = calendar.monthrange(year, month)[1]
    day = min(closing_day, last_day)
    return f"{year:04d}-{month:02d}-{day:02d}"


def _format_amount(val: Any) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    try:
        n = float(str(val).replace(",", "").strip())
    except (TypeError, ValueError) as exc:
        raise TransformError(f"Invalid amount: {val!r}") from exc
    if abs(n - round(n, 2)) < 1e-9:
        n = round(n, 2)
    if n == int(n):
        return str(int(n))
    return f"{n:.2f}".rstrip("0").rstrip(".")


def _blank_str(val: Any) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    s = str(val).strip()
    return "" if s.lower() == "nan" else s


def _line_memo(
    row: pd.Series,
    cols: Dict[str, Optional[str]],
    prefix: str,
) -> str:
    evt = _blank_str(row[cast(str, cols["accounting_event_type"])])
    acct = _blank_str(row[cast(str, cols["account_name"])])
    pid_col = cols.get("product_id")
    pid = _blank_str(row[cast(str, pid_col)]) if pid_col else ""
    product_label = pid or acct
    return f"{prefix} - {evt} | {product_label}" if product_label else f"{prefix} - {evt}"


def _revenue_category(row: pd.Series, cols: Dict[str, Optional[str]]) -> str:
    pcode = cols.get("product_code")
    ptype_col = cols.get("product_type")
    code = _blank_str(row[cast(str, pcode)]) if pcode else ""
    ptype = _blank_str(row[cast(str, ptype_col)]) if ptype_col else ""
    if code and ptype:
        return f"{code} - {ptype}"
    return code or ptype


def _cost_center_cell(
    line_company: str,
    cost_center_id: str,
    pattern: Optional[str],
) -> str:
    if pattern:
        try:
            return pattern.format(
                line_company_reference_id=line_company or "",
                cost_center_id=cost_center_id,
            )
        except KeyError as exc:
            raise TransformError(
                f"Invalid worktag_cost_center_pattern placeholder: {exc}"
            ) from exc
    if line_company and cost_center_id:
        return f"{line_company}_{cost_center_id}"
    return cost_center_id


def _resolve_input_columns(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    """Map required/optional logical names to df column names."""
    return {
        "actg_period": _require_column(df, "Actg Period", "actg_period"),
        "entry_type": _require_column(df, "Type", "entry_type", "Debit/Credit"),
        "amount": _require_column(df, "Amount", "amount"),
        "currency": _require_column(df, "Currency Code", "currency_code", "Currency"),
        "ledger_account": _require_column(
            df, "Account Number", "account_number", "LedgerAccount"
        ),
        "accounting_event_type": _require_column(
            df, "Accounting Event Type", "accounting_event_type"
        ),
        "account_name": _require_column(df, "Account Name", "account_name"),
        "product_id": _find_column(df, "Product Id", "Product ID", "product_id"),
        "product_code": _find_column(df, "Product Code", "product_code", "CF Product Code"),
        "product_type": _find_column(df, "Product Type", "product_type", "CF Product Type"),
        "market_id": _find_column(
            df,
            "MarketID Finance",
            "Market ID Finance",
            "cf_MarketID_Finance",
            "Market Id",
            "market_id_finance",
        ),
    }


def detect_revrec_journal_export(df: pd.DataFrame) -> bool:
    """True if df has GL-style columns for auto journal mode."""
    keys = {_norm_header(c) for c in df.columns}
    return (
        "actg_period" in keys
        and "account_number" in keys
        and "type" in keys
        and "amount" in keys
    )


def resolve_transform_mode(df: pd.DataFrame, config: Mapping[str, Any]) -> str:
    """Return workday_journal, passthrough, or infer when mode is auto."""
    mode = config.get("transform_mode") or "auto"
    if mode == "workday_journal":
        return "workday_journal"
    if mode == "passthrough":
        return "passthrough"
    if mode == "auto":
        return "workday_journal" if detect_revrec_journal_export(df) else "passthrough"
    raise TransformError(
        f"transform_mode must be one of auto, workday_journal, passthrough; got {mode!r}"
    )


def _build_empty_workday_row() -> Dict[str, str]:
    """One output row with every column set to empty (oracle-style base row)."""
    return dict.fromkeys(WORKDAY_OUTPUT_COLUMNS, "")


def _transform_workday_row(
    pos: int,
    row: pd.Series,
    cols: Dict[str, Optional[str]],
    cfg: WorkdayJournalBuildConfig,
    line_order: int,
) -> Dict[str, str]:
    """Map one input row to journal output columns; missing optional fields stay ``\"\"``."""
    out = _build_empty_workday_row()
    closing_day = cfg.accounting_date_closing_day

    try:
        acct_date = _actg_period_to_accounting_date(
            row[cast(str, cols["actg_period"])], closing_day
        )
    except TransformError as exc:
        raise TransformError(f"Line {pos}: {exc}") from exc

    type_col = cast(str, cols["entry_type"])
    et_raw = str(row[type_col]).strip().casefold()
    if et_raw not in ("debit", "credit"):
        raise TransformError(
            f"Line {pos}: Type must be Debit or Credit, got {row[type_col]!r}"
        )

    try:
        amt = _format_amount(row[cast(str, cols["amount"])])
    except TransformError as exc:
        raise TransformError(f"Line {pos}: {exc}") from exc

    debit = amt if et_raw == "debit" else ""
    credit = amt if et_raw == "credit" else ""

    mcol = cols.get("market_id")
    line_company = _blank_str(row[cast(str, mcol)]) if mcol else ""
    ledger_id = _blank_str(row[cast(str, cols["ledger_account"])])
    cur = _blank_str(row[cast(str, cols["currency"])])

    out["JournalKey"] = cfg.journal_key
    out["JournalEntryMemo"] = cfg.journal_entry_memo
    out["Submit"] = WORKDAY_DEFAULT_SUBMIT
    out["CompanyReferenceIDType"] = cfg.company_reference_id_type
    out["CompanyReferenceID"] = cfg.company_reference_id
    out["Currency"] = cur
    out["LedgerType"] = cfg.ledger_type
    out["AccountingDate"] = acct_date
    out["JournalSource"] = cfg.journal_source
    out["AdjustmentJournal"] = WORKDAY_DEFAULT_ADJUSTMENT_JOURNAL
    out["CurrencyRateType"] = WORKDAY_DEFAULT_CURRENCY_RATE_TYPE
    out["JournalLineOrder"] = str(line_order)
    out["LineCompanyReferenceIDType"] = cfg.line_company_reference_id_type
    out["LineCompanyReferenceID"] = line_company
    out["LedgerAccountReferenceID_ParentIDType"] = cfg.ledger_account_parent_id_type
    out["LedgerAccountReferenceID_ParentID"] = cfg.ledger_account_parent_id
    out["LedgerAccountReferenceIDType"] = cfg.ledger_account_reference_id_type
    out["LedgerAccountReferenceID"] = ledger_id
    out["LineMemo"] = _line_memo(row, cols, cfg.journal_line_memo_prefix)
    out["DebitAmount"] = debit
    out["CreditAmount"] = credit
    out["LineCurrency"] = cur
    out["LineCurrencyRate"] = WORKDAY_DEFAULT_LINE_CURRENCY_RATE
    out["LedgerDebitAmount"] = debit
    out["LedgerCreditAmount"] = credit
    out["ExcludeFromSpendReport"] = WORKDAY_DEFAULT_EXCLUDE_FROM_SPEND

    rev = _revenue_category(row, cols)
    if rev:
        out["Worktag_Revenue_Category_ID"] = rev

    if cols.get("product_type"):
        pt = _blank_str(row[cast(str, cols["product_type"])])
        if pt:
            out["Worktag_Sales_Item_ID"] = pt

    cc = _cost_center_cell(
        line_company,
        cfg.worktag_cost_center_id,
        cfg.worktag_cost_center_pattern,
    )
    if cc:
        out["Worktag_Cost_Center_Reference_ID"] = cc

    return out


def build_workday_journal_dataframe(
    df: pd.DataFrame,
    cfg: WorkdayJournalBuildConfig,
) -> pd.DataFrame:
    """Build journal-layout output dataframe."""
    if df.empty:
        raise TransformError("Journal summary contains no data rows.")

    cols = _resolve_input_columns(df)

    rows_out: List[Dict[str, str]] = []
    for pos, (_, row) in enumerate(df.iterrows(), start=1):
        line_order = len(rows_out) + 1
        rows_out.append(_transform_workday_row(pos, row, cols, cfg, line_order))

    out = pd.DataFrame(rows_out, columns=WORKDAY_OUTPUT_COLUMNS)
    logger.info("Journal rows built: input_rows=%s output_rows=%s", len(df), len(out))
    return out


def transform_journal_summary(config: Dict[str, Any]) -> Tuple[Path, Optional[Path]]:
    """Transform and write CSV; returns output path and temp dir if any."""
    input_path = Path(config["journal_summary_input_path"]).expanduser().resolve()
    if not input_path.is_file():
        raise TransformError(f"Journal summary file not found: {input_path}")

    logger.info("Reading journal summary path=%s", input_path)
    try:
        df = pd.read_csv(input_path)
    except pd.errors.EmptyDataError as exc:
        raise TransformError(
            f"Journal summary file is empty or has no parsable CSV content: {input_path}"
        ) from exc
    except pd.errors.ParserError as exc:
        raise TransformError(f"Failed to parse journal summary CSV: {exc}") from exc
    except OSError as exc:
        raise TransformError(f"Failed to read journal summary CSV: {exc}") from exc

    logger.info("Loaded journal summary rows=%s columns=%s", len(df), len(df.columns))

    column_map = _coerce_column_map(config.get("column_map"))
    if column_map:
        missing = set(column_map) - set(df.columns)
        if missing:
            raise TransformError(
                f"column_map references unknown columns: {sorted(missing)}; "
                f"available={list(df.columns)}"
            )
        df = df.rename(columns=column_map)

    mode = resolve_transform_mode(df, config)
    logger.info("Transform mode=%s", mode)

    if mode == "workday_journal":
        wd_cfg = WorkdayJournalBuildConfig.from_target_config(config)
        df = build_workday_journal_dataframe(df, wd_cfg)
    else:
        columns_order = _coerce_columns_order(config.get("columns_order"))
        if columns_order:
            missing = [c for c in columns_order if c not in df.columns]
            if missing:
                raise TransformError(
                    f"columns_order references unknown columns: {missing}; "
                    f"available={list(df.columns)}"
                )
            df = df[columns_order]

    sep = str(config.get("output_delimiter") or ",")
    if len(sep) != 1:
        raise TransformError("output_delimiter must be a single character")

    out_name = config.get("transform_output_filename") or f"workday_journal_{input_path.stem}.csv"
    out_dir_raw = config.get("transform_output_dir")
    temp_root: Optional[Path] = None
    if out_dir_raw:
        out_dir = Path(out_dir_raw).expanduser().resolve()
        out_dir.mkdir(parents=True, exist_ok=True)
    else:
        out_dir = Path(tempfile.mkdtemp(prefix="journal-transform-"))
        temp_root = out_dir

    out_path = out_dir / out_name
    logger.info("Writing transformed file path=%s sep=%r", out_path, sep)
    try:
        df.to_csv(out_path, index=False, sep=sep)
    except OSError as exc:
        raise TransformError(f"Failed to write transformed file: {exc}") from exc

    logger.info("Transform complete rows_written=%s", len(df))
    return out_path, temp_root
