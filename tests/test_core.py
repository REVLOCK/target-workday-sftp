"""Package tests."""

from __future__ import annotations

import csv
import json

import pytest

from target_workday_sftp import flatten_config, normalize_target_config
from target_workday_sftp.const import (
    INPUT_FILENAME,
    REQUIRED_CONFIG_KEYS,
    REQUIRED_FLATTENED_CONFIG_KEYS,
    REQUIRED_INPUT_COLUMNS,
    TRANSFORM_OUTPUT_DIR_DEFAULT,
    TRANSFORM_OUTPUT_FILENAME,
)
from target_workday_sftp.exceptions import InputError, TransformError
from target_workday_sftp.transform import transform_journal_summary

_JOURNAL_CSV_HEADER = (
    "Transaction Date,Journal Entry Id,Account Number,Account Name,Amount,"
    "Posting Type,Currency,ProductType,Product Code,MarketID Finance,Customer Name,Description\n"
)

_WORKDAY_JOURNAL_FLAGS = {
    "JournalEntryMemo": "Chargebee RevRec",
    "JournalSource": "Chargebee RevRec",
    "Submit": "1",
    "AdjustmentJournal": "1",
    "CurrencyRateType": "Current",
    "ExcludeFromSpendReport": "1",
}


def _write_input_workspace(tmp_path, name: str, csv_body: str):
    """Write INPUT_FILENAME under tmp_path/name."""
    root = tmp_path / name
    root.mkdir(parents=True, exist_ok=True)
    (root / INPUT_FILENAME).write_text(csv_body, encoding="utf-8")
    return root


def test_required_config_keys_defined() -> None:
    assert "input_path" in REQUIRED_CONFIG_KEYS
    assert "input_path" in REQUIRED_FLATTENED_CONFIG_KEYS
    assert "sftp_host" in REQUIRED_FLATTENED_CONFIG_KEYS
    assert REQUIRED_INPUT_COLUMNS[0] == "Transaction Date"
    assert "ProductType" in REQUIRED_INPUT_COLUMNS


def test_transform_default_output_filename(tmp_path) -> None:
    """Default output filename is chargebee_journal_posting.csv."""
    jroot = _write_input_workspace(
        tmp_path,
        "revrec_in",
        _JOURNAL_CSV_HEADER + "2025-09-01,je-1,10000,AR,1,Credit,USD,,,,,memo\n",
    )
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    config = {
        "input_path": str(jroot),
        "transform_output_dir": str(out_dir),
        **_WORKDAY_JOURNAL_FLAGS,
    }
    out_path = transform_journal_summary(config)
    assert out_path.name == TRANSFORM_OUTPUT_FILENAME


def test_transform_default_output_dir_relative_to_cwd(tmp_path, monkeypatch) -> None:
    """Default output dir is ./output from cwd."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / INPUT_FILENAME).write_text(
        _JOURNAL_CSV_HEADER + "2024-01-01,je,10000,AR,10.5,Credit,USD,,,,,m\n",
        encoding="utf-8",
    )
    config = {
        "input_path": str(tmp_path),
        **_WORKDAY_JOURNAL_FLAGS,
    }
    out_path = transform_journal_summary(config)
    assert out_path == tmp_path / TRANSFORM_OUTPUT_DIR_DEFAULT / TRANSFORM_OUTPUT_FILENAME
    assert out_path.is_file()


def test_config_fields_blob_merges_into_flat_config() -> None:
    dept = {
        "JournalKey": "9",
        "JournalEntryMemo": "Dept Memo",
        "JournalSource": "Dept Source",
        "CompanyReferenceID": "XYZ",
        "Submit": "1",
        "AdjustmentJournal": "1",
        "CurrencyRateType": "Current",
        "ExcludeFromSpendReport": "1",
        "LineMemo": "Default line memo",
        "Worktag_Cost_Center_Reference_ID": "500",
    }
    raw = {
        "input_path": "/tmp",
        "config_fields": json.dumps(dept),
    }
    cfg = flatten_config(raw)
    assert cfg["JournalKey"] == "9"
    assert cfg["JournalEntryMemo"] == "Dept Memo"
    assert cfg["JournalSource"] == "Dept Source"
    assert cfg["CompanyReferenceID"] == "XYZ"
    assert cfg["Worktag_Cost_Center_Reference_ID"] == "500"
    assert cfg["Submit"] == "1"
    assert cfg["AdjustmentJournal"] == "1"
    assert cfg["CurrencyRateType"] == "Current"
    assert cfg["ExcludeFromSpendReport"] == "1"
    assert cfg["LineMemo"] == "Default line memo"


def test_flatten_config_fields_overridden_by_top_level() -> None:
    raw = {
        "input_path": "/tmp/j",
        "config_fields": json.dumps({"Submit": "0", "JournalKey": "9"}),
        "Submit": "1",
        "AdjustmentJournal": "1",
        "CurrencyRateType": "Current",
        "ExcludeFromSpendReport": "1",
    }
    cfg = flatten_config(raw)
    assert cfg["Submit"] == "1"
    assert cfg["JournalKey"] == "9"


def test_normalize_target_config_is_flatten_alias() -> None:
    raw = {"input_path": "/x"}
    assert normalize_target_config(raw) == flatten_config(raw)


def test_market_id_finance_line_company_raw_and_cost_center_suffix_400(tmp_path) -> None:
    """LineCompanyReferenceID is raw MarketID Finance; Worktag_Cost_Center_Reference_ID gets _400 suffix."""
    jroot = _write_input_workspace(
        tmp_path,
        "mid",
        _JOURNAL_CSV_HEADER
        + "2025-09-01,je,10000,AR,200,Credit,USD,,,ACME,,memo\n",
    )
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    config = {
        "input_path": str(jroot),
        "transform_output_dir": str(out_dir),
        "LineCompanyReferenceID": "IGNORED_WHEN_MARKET_SET",
        **_WORKDAY_JOURNAL_FLAGS,
    }
    out_path = transform_journal_summary(config)
    with out_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["LineCompanyReferenceID"] == "ACME"
    assert rows[0]["Worktag_Cost_Center_Reference_ID"] == "ACME_400"


def test_blank_row_cells_output_empty_row_fields(tmp_path) -> None:
    """Currency stays row-only; blank MarketID → empty LineCompanyReferenceID; cost center uses config LineCompanyReferenceID."""
    jroot = _write_input_workspace(
        tmp_path,
        "fb",
        _JOURNAL_CSV_HEADER + "2025-09-01,je,10000,AR,200,Credit,,,,,,\n",
    )
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    config = {
        "input_path": str(jroot),
        "transform_output_dir": str(out_dir),
        "Currency": "USD",
        "LineCurrency": "USD",
        "LineCompanyReferenceID": "BUFF",
        **_WORKDAY_JOURNAL_FLAGS,
    }
    out_path = transform_journal_summary(config)
    with out_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["Currency"] == ""
    assert rows[0]["LineCurrency"] == ""
    assert rows[0]["LineCompanyReferenceID"] == ""
    assert rows[0]["Worktag_Cost_Center_Reference_ID"] == "BUFF"


def test_spend_category_json_map_matches_account_number(tmp_path) -> None:
    """When ``spend_category`` JSON maps Account Number, output uses that label."""
    spend_map = json.dumps({"5510": "Bad Debts", "5435": "Delivery & Express Postage"})
    jroot = _write_input_workspace(
        tmp_path,
        "spend",
        _JOURNAL_CSV_HEADER + "2025-09-01,je,5510,AR,50,Credit,USD,,,,,m\n",
    )
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    config = {
        "input_path": str(jroot),
        "transform_output_dir": str(out_dir),
        "spend_category": spend_map,
        "Worktag_Spend_Category_ID": "IGNORED_WHEN_MAP_MATCHES",
        **_WORKDAY_JOURNAL_FLAGS,
    }
    out_path = transform_journal_summary(config)
    with out_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["LedgerAccountReferenceID"] == "5510"
    assert rows[0]["Worktag_Spend_Category_ID"] == "Bad Debts"
    assert rows[0]["Worktag_Revenue_Category_ID"] == ""
    assert rows[0]["Worktag_Sales_Item_ID"] == ""


def test_spend_category_no_account_match_uses_config_worktag_spend(tmp_path) -> None:
    """Account Number not in ``spend_category`` map → ``Worktag_Spend_Category_ID`` from config."""
    spend_map = json.dumps({"5510": "Bad Debts"})
    jroot = _write_input_workspace(
        tmp_path,
        "spend_nomatch",
        _JOURNAL_CSV_HEADER + "2025-09-01,je,9999,AR,50,Credit,USD,,,,,m\n",
    )
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    config = {
        "input_path": str(jroot),
        "transform_output_dir": str(out_dir),
        "spend_category": spend_map,
        "Worktag_Spend_Category_ID": "FROM_CONFIG",
        **_WORKDAY_JOURNAL_FLAGS,
    }
    out_path = transform_journal_summary(config)
    with out_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["Worktag_Spend_Category_ID"] == "FROM_CONFIG"


def test_spend_category_skipped_for_accounts_1170_and_1180(tmp_path) -> None:
    """Accounts 1170/1180 skip spend map and revenue/sales worktag logic; spend stays from config."""
    spend_map = json.dumps({"1170": "FROM_MAP", "1180": "FROM_MAP_1180", "5510": "Other"})
    header = (
        "Transaction Date,Journal Entry Id,Account Number,Account Name,Amount,"
        "Posting Type,Currency,ProductType,Product Code,MarketID Finance,Customer Name,Description,"
        "Worktag Revenue Category ID,Worktag Sales Item ID\n"
    )
    body = (
        "2025-09-01,je1,1170,AR,50,Credit,USD,,,,,m,REV_SHOULD_SKIP,SALES_SKIP\n"
        "2025-09-01,je2,1180,AR,50,Credit,USD,,,,,m,REV2_SKIP,SALES2_SKIP\n"
    )
    jroot = _write_input_workspace(tmp_path, "spend_skip_117x", header + body)
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    config = {
        "input_path": str(jroot),
        "transform_output_dir": str(out_dir),
        "spend_category": spend_map,
        "Worktag_Spend_Category_ID": "CONFIG_SPEND",
        **_WORKDAY_JOURNAL_FLAGS,
    }
    out_path = transform_journal_summary(config)
    with out_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["LedgerAccountReferenceID"] == "1170"
    assert rows[0]["Worktag_Spend_Category_ID"] == "CONFIG_SPEND"
    assert rows[0]["Worktag_Revenue_Category_ID"] == ""
    assert rows[0]["Worktag_Sales_Item_ID"] == ""
    assert rows[1]["LedgerAccountReferenceID"] == "1180"
    assert rows[1]["Worktag_Spend_Category_ID"] == "CONFIG_SPEND"
    assert rows[1]["Worktag_Revenue_Category_ID"] == ""
    assert rows[1]["Worktag_Sales_Item_ID"] == ""


def test_transform_chargebee_transaction_date_shape(tmp_path) -> None:
    """Fixed journal headers: period, type, amounts, line memo text."""
    jroot = _write_input_workspace(
        tmp_path,
        "cb_td",
        _JOURNAL_CSV_HEADER
        + "2025-03-31,je1,2600,Deferred,160,Credit,USD,SUBSCRIPTION,2,,,202503 Revenue\n",
    )
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    config = {
        "input_path": str(jroot),
        "transform_output_dir": str(out_dir),
        **_WORKDAY_JOURNAL_FLAGS,
    }
    out_path = transform_journal_summary(config)
    with out_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["LedgerAccountReferenceID"] == "2600"
    assert rows[0]["CreditAmount"] == "160.0"
    assert rows[0]["DebitAmount"] == ""
    assert rows[0]["LineMemo"] == "Chargebee RevRec | SUBSCRIPTION"


def test_transform_workday_journal_auto(tmp_path) -> None:
    jroot = _write_input_workspace(
        tmp_path,
        "auto",
        _JOURNAL_CSV_HEADER
        + "2025-09-15,je1,10000,AR,200,Credit,USD,,,,,d\n"
        + "2025-09-16,je2,10000,AR,1413,Debit,USD,,,,,d\n",
    )
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    config = {
        "input_path": str(jroot),
        "transform_output_dir": str(out_dir),
        **_WORKDAY_JOURNAL_FLAGS,
    }
    out_path = transform_journal_summary(config)
    with out_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["JournalEntryMemo"] == "Chargebee RevRec"
    assert rows[0]["AccountingDate"] == "2025-09-15"
    assert rows[0]["CreditAmount"] == "200.0"
    assert rows[0]["DebitAmount"] == ""
    assert rows[1]["AccountingDate"] == "2025-09-16"
    assert rows[1]["DebitAmount"] == "1413.0"
    assert rows[1]["CreditAmount"] == ""


def test_transform_workday_empty_input(tmp_path) -> None:
    jroot = _write_input_workspace(
        tmp_path,
        "empty",
        _JOURNAL_CSV_HEADER,
    )
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    config = {
        "input_path": str(jroot),
        "transform_output_dir": str(out_dir),
        **_WORKDAY_JOURNAL_FLAGS,
    }
    with pytest.raises(InputError, match="Input CSV has no data rows"):
        transform_journal_summary(config)


def test_transform_accounting_date_normalized_to_yyyy_mm_dd(tmp_path) -> None:
    """AccountingDate is YYYY-MM-DD (slash dates, ISO datetime date part)."""
    jroot = _write_input_workspace(
        tmp_path,
        "dates",
        _JOURNAL_CSV_HEADER
        + "2026/04/04,je1,10000,AR,1,Credit,USD,,,,,a\n"
        + "04/05/2026,je2,10000,AR,1,Debit,USD,,,,,b\n"
        + "2026-06-07T14:30:00,je3,10000,AR,1,Credit,USD,,,,,c\n",
    )
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    config = {
        "input_path": str(jroot),
        "transform_output_dir": str(out_dir),
        **_WORKDAY_JOURNAL_FLAGS,
    }
    out_path = transform_journal_summary(config)
    with out_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["AccountingDate"] == "2026-04-04"
    assert rows[1]["AccountingDate"] == "2026-04-05"
    assert rows[2]["AccountingDate"] == "2026-06-07"


def test_transform_passes_empty_transaction_date_to_accounting_date(tmp_path) -> None:
    """AccountingDate blank when Transaction Date is blank."""
    jroot = _write_input_workspace(
        tmp_path,
        "no_tx",
        _JOURNAL_CSV_HEADER + ",je,10000,AR,200,Credit,USD,,,,,memo\n",
    )
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    config = {
        "input_path": str(jroot),
        "transform_output_dir": str(out_dir),
        **_WORKDAY_JOURNAL_FLAGS,
    }
    out_path = transform_journal_summary(config)
    with out_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["AccountingDate"] == ""


