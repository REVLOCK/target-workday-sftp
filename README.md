# target-workday-sftp

Batch-style target (same pattern as [target-intacct](https://github.com/hotgluexyz/target-intacct)): read a **RevRec journal summary** file from disk, **transform** it to the layout your Workday/SFTP consumer expects, then **upload** the result over **SFTP**.

## Quick Start

1. **Install**

   ```bash
   pip install git+https://github.com/hotgluexyz/target-workday-sftp.git
   ```

   Or from a clone:

   ```bash
   pip install -e ".[dev]"
   ```

2. **Config**

   Copy `sample_config.json` to `config.json` and set at least:

   | Key | Purpose |
   |-----|---------|
   | `input_path` | Directory containing `JournalEntries.csv`. The CSV must use the exact column names in `REQUIRED_INPUT_COLUMNS` in `const.py` (default journal export: `Transaction Date`, `Journal Entry Id`, `Account Number`, `Account Name`, `Amount`, `Posting Type`, `Currency`, `ProductType`, `Product Code`, `MarketID Finance`, `Customer Name`, `Description`). Required after flatten (top level or via `config_fields`). |
   | `sftp_host` | SFTP server hostname. |
   | `sftp_username` | SFTP login. |
   | `sftp_remote_path` | Remote **file path**, or a **directory** ending with `/` (basename of the transformed file is appended). |
   | `sftp_password` **or** `sftp_private_key_path` | Authentication (one or both as required by your server). |

   Optional:

   - `transform_output_dir` — where to write the transformed file before upload; if omitted or empty, `./output` (created if needed). The local file is deleted after the SFTP upload attempt (success or failure). The file basename is always **`YYYYMMDD.csv`** using the **UTC** calendar date (see `TRANSFORM_OUTPUT_DEFAULT_DATE_STRFTIME` in `const.py`); multiple runs the same UTC day overwrite the same file.
   - **AccountingDate** in the output is copied from the journal **`Transaction Date`** cell (`_blank_str`: trimmed, `\N`-like → empty). Same string as the export when present (typically **`YYYY-MM-DD`**).
   - `config_fields` — JSON **object** or **string** of an object: keys merged onto the flat config (usually Workday journal CSV header names and related defaults). Applied before top-level keys (so top-level wins). Empty strings are ignored. Nothing in `const.py` validates the key set—transform reads what it needs from the merged flat dict. Row-driven columns (`Currency`, `LedgerAccountReferenceID`, `LineMemo`, revenue/sales worktags, amounts, etc.) come **only** from the journal row (trimmed); blank cells stay blank in the output. **`LineCompanyReferenceID`** is the journal **`MarketID Finance`** cell when non-blank; otherwise it uses **`LineCompanyReferenceID`** from the flattened config (if set). Other Workday columns are set from the flattened config via `_str_from_config` (same idea as Oracle for config-only GL fields). Top-level-only settings (`journal_line_memo_prefix`, transform paths, SFTP keys, etc.) stay outside this blob.
   `flatten_config` / `normalize_target_config` perform that merge; `REQUIRED_FLATTENED_CONFIG_KEYS` in `const.py` enforces **input path + SFTP** only. Workday journal fields such as `Submit`, `AdjustmentJournal`, `CurrencyRateType`, and `ExcludeFromSpendReport` are optional strings from the flattened config (including `config_fields`); if omitted or blank, the output CSV contains empty values like other config-only columns. `REQUIRED_CONFIG_KEYS` is only what Singer validates on the raw config (`input_path`).

3. **Run**

   ```bash
   target-workday-sftp --config config.json
   ```

## Hotglue

This target is **classic Singer** (`singer-python`: config file, batch transform, SFTP). It is **not** a **Singer SDK** (`singer-sdk` / Meltano SDK) target.

In your Hotglue **connector / target definition**, set **`singer_sdk` to `false`** (or turn off the equivalent “Singer SDK” toggle). If it is left `true`, the executor may treat stdio and lifecycle like an SDK target and you can see spurious failures (e.g. exit code **141**) even when the same package runs fine locally.

## Layout

- `setup.py` / `setup.cfg` — package metadata and entry point.
- `target_workday_sftp/__init__.py` — Singer `main()`, `flatten_config` (`config_fields` then top-level), `normalize_target_config`, `require_flattened_config`.
- `target_workday_sftp/__main__.py` — `python -m target_workday_sftp`.
- `target_workday_sftp/transform.py` — CSV column resolution, config helpers (`_str_from_config`), Workday row mapping.
- `target_workday_sftp/sftp_upload.py` — SFTP client and upload.
- `target_workday_sftp/const.py` — keys, columns, timeouts.
- `target_workday_sftp/exceptions.py` — errors.
- `sample_config.json` — sample config.

## Developer Resources

```bash
pip install -e ".[dev]"
pytest
```
