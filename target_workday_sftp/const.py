"""Config key lists, timeouts, output column order."""

# Validated after optional SFTP key aliases are applied.
REQUIRED_CONFIG_KEYS = [
    "journal_summary_input_path",
    "sftp_host",
    "sftp_username",
    "sftp_remote_path",
]

OPTIONAL_CONFIG_KEYS = [
    "sftp_port",
    "sftp_password",
    "sftp_private_key_path",
    "sftp_private_key_passphrase",
    "host",
    "username",
    "password",
    "port",
    "remote_path",
    "folder",
    "transform_mode",
    "journal_key",
    "journal_entry_memo",
    "journal_source",
    "journal_line_memo_prefix",
    "company_reference_id_type",
    "company_reference_id",
    "ledger_type",
    "line_company_reference_id_type",
    "ledger_account_parent_id_type",
    "ledger_account_parent_id",
    "ledger_account_reference_id_type",
    "worktag_cost_center_id",
    "worktag_cost_center_pattern",
    "accounting_date_closing_day",
    "column_map",
    "output_delimiter",
    "transform_output_filename",
    "transform_output_dir",
    "columns_order",
]

SFTP_CONNECT_TIMEOUT = 60
SFTP_BANNER_TIMEOUT = 60
SFTP_AUTH_TIMEOUT = 60

# Output CSV column order (flat journal file).
WORKDAY_OUTPUT_COLUMNS = [
    "JournalKey",
    "JournalEntryMemo",
    "Submit",
    "CompanyReferenceIDType",
    "CompanyReferenceID",
    "Currency",
    "LedgerType",
    "BookCode",
    "AccountingDate",
    "JournalSource",
    "AdjustmentJournal",
    "BalancingWorktagReferenceIDType",
    "CurrencyRateType",
    "JournalLineOrder",
    "LineCompanyReferenceIDType",
    "LineCompanyReferenceID",
    "LedgerAccountReferenceID_ParentIDType",
    "LedgerAccountReferenceID_ParentID",
    "LedgerAccountReferenceIDType",
    "LedgerAccountReferenceID",
    "LineMemo",
    "DebitAmount",
    "CreditAmount",
    "LineCurrency",
    "LineCurrencyRate",
    "LedgerDebitAmount",
    "LedgerCreditAmount",
    "ExcludeFromSpendReport",
    "Worktag_Revenue_Category_ID",
    "Worktag_Sales_Item_ID",
    "Worktag_Cost_Center_Reference_ID",
    "Worktag_Location_ID",
    "Worktag_Project_ID",
    "Worktag_Spend_Category_ID",
    "Worktag_Employee_ID",
    "Worktag_Customer_ID",
    "Worktag_Organization_Reference_ID",
    "Worktag_Bank_Account_ID",
    "ExternalCode_RegionMap",
    "ExternalCode_CustomerIDMap",
]

# Default cell values for journal rows (literals live in const like target_oracle_fusion).
WORKDAY_DEFAULT_SUBMIT = "1"
WORKDAY_DEFAULT_ADJUSTMENT_JOURNAL = "1"
WORKDAY_DEFAULT_CURRENCY_RATE_TYPE = "Current"
WORKDAY_DEFAULT_LINE_CURRENCY_RATE = "1"
WORKDAY_DEFAULT_EXCLUDE_FROM_SPEND = "1"
