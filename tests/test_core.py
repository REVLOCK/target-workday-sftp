"""Unit tests for transform and SFTP config helpers."""

from __future__ import annotations

import csv
import pytest

from target_workday_sftp.sftp_upload import normalize_target_config
from target_workday_sftp.const import REQUIRED_CONFIG_KEYS
from target_workday_sftp.exceptions import TransformError
from target_workday_sftp.transform import transform_journal_summary


def test_required_config_keys_defined() -> None:
    assert "journal_summary_input_path" in REQUIRED_CONFIG_KEYS
    assert "sftp_host" in REQUIRED_CONFIG_KEYS


def test_transform_column_map(tmp_path) -> None:
    src = tmp_path / "revrec.csv"
    src.write_text("RevRecAmount,RevRecPeriod\n10.5,2024-01\n", encoding="utf-8")
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    config = {
        "journal_summary_input_path": str(src),
        "column_map": {"RevRecAmount": "Amount", "RevRecPeriod": "AccountingPeriod"},
        "transform_output_dir": str(out_dir),
        "transform_output_filename": "upload.csv",
        "output_delimiter": ",",
    }
    out_path, temp_root = transform_journal_summary(config)
    assert temp_root is None
    assert out_path.name == "upload.csv"
    text = out_path.read_text(encoding="utf-8")
    assert "Amount" in text and "AccountingPeriod" in text


def test_normalize_target_config_sftp_aliases() -> None:
    raw = {
        "journal_summary_input_path": "/tmp/in.csv",
        "host": "cltftp.example.com",
        "username": "ChargeB39",
        "password": "secret",
        "remote_path": "/home/ChargeB39/Dev/",
    }
    cfg = normalize_target_config(raw)
    assert cfg["sftp_host"] == "cltftp.example.com"
    assert cfg["sftp_username"] == "ChargeB39"
    assert cfg["sftp_password"] == "secret"
    assert cfg["sftp_remote_path"] == "/home/ChargeB39/Dev/"


def test_transform_workday_journal_auto(tmp_path) -> None:
    src = tmp_path / "revrec.csv"
    src.write_text(
        "OrgId,Actg Period,Accounting Event Type,Account Name,Account Number,"
        "Type,Currency Code,Amount\n"
        "1,202509,A/R,AR,10000,Credit,USD,200\n"
        "1,202509,A/R,AR,10000,Debit,USD,1413\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    config = {
        "journal_summary_input_path": str(src),
        "transform_output_dir": str(out_dir),
        "transform_output_filename": "upload.csv",
        "output_delimiter": ",",
    }
    out_path, temp_root = transform_journal_summary(config)
    assert temp_root is None
    with out_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["JournalEntryMemo"] == "Chargebee RevRec"
    assert rows[0]["AccountingDate"] == "2025-09-15"
    assert rows[0]["CreditAmount"] == "200"
    assert rows[0]["DebitAmount"] == ""
    assert rows[1]["DebitAmount"] == "1413"
    assert rows[1]["CreditAmount"] == ""


def test_transform_workday_empty_input(tmp_path) -> None:
    src = tmp_path / "empty.csv"
    src.write_text(
        "OrgId,Actg Period,Accounting Event Type,Account Name,Account Number,"
        "Type,Currency Code,Amount\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    config = {
        "journal_summary_input_path": str(src),
        "transform_output_dir": str(out_dir),
        "transform_output_filename": "out.csv",
        "output_delimiter": ",",
    }
    with pytest.raises(TransformError, match="no data rows"):
        transform_journal_summary(config)


def test_workday_invalid_closing_day(tmp_path) -> None:
    src = tmp_path / "revrec.csv"
    src.write_text(
        "OrgId,Actg Period,Accounting Event Type,Account Name,Account Number,"
        "Type,Currency Code,Amount\n"
        "1,202509,A/R,AR,10000,Credit,USD,200\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    config = {
        "journal_summary_input_path": str(src),
        "transform_output_dir": str(out_dir),
        "accounting_date_closing_day": 99,
        "output_delimiter": ",",
    }
    with pytest.raises(TransformError, match="1-31"):
        transform_journal_summary(config)


def test_transform_unknown_column_in_map(tmp_path) -> None:
    src = tmp_path / "revrec.csv"
    src.write_text("a,b\n1,2\n", encoding="utf-8")
    config = {
        "journal_summary_input_path": str(src),
        "column_map": {"missing": "x"},
        "transform_output_dir": str(tmp_path),
    }
    with pytest.raises(TransformError):
        transform_journal_summary(config)
