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
   | `journal_summary_input_path` | Path to the RevRec export (CSV). |
   | `sftp_host` | SFTP server hostname. |
   | `sftp_username` | SFTP login. |
   | `sftp_remote_path` | Remote **file path**, or a **directory** ending with `/` (basename of the transformed file is appended). |
   | `sftp_password` **or** `sftp_private_key_path` | Authentication (one or both as required by your server). |

   Optional:

   - `column_map` — rename columns from RevRec names to Workday-facing names (object or JSON string).
   - `columns_order` — list of columns to keep and order in the output file.
   - `output_delimiter` — default `,` (single character).
   - `transform_output_filename` — output name (default `workday_journal_<stem>.csv`).
   - `transform_output_dir` — where to write the transformed file; if empty, a temp directory is used and removed after upload.

   Required keys are listed in `src/target_workday_sftp/const.py` as `REQUIRED_CONFIG_KEYS`.

3. **Run**

   ```bash
   target-workday-sftp --config config.json
   ```

## Layout

- `setup.py` / `setup.cfg` — package metadata and entry point.
- `src/target_workday_sftp/__init__.py` — Singer `main()`.
- `src/target_workday_sftp/__main__.py` — `python -m target_workday_sftp`.
- `src/target_workday_sftp/transform.py` — CSV transform (journal + passthrough).
- `src/target_workday_sftp/sftp_upload.py` — config load, SFTP aliases, upload.
- `src/target_workday_sftp/const.py` — keys, columns, timeouts.
- `src/target_workday_sftp/exceptions.py` — errors.
- `sample_config.json` — sample config.

## Developer Resources

```bash
pip install -e ".[dev]"
pytest
```
