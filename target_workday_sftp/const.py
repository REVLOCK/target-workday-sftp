"""Constants for config keys, paths, timeouts, output columns."""

TRANSFORM_OUTPUT_DIR_DEFAULT = "output"
TRANSFORM_OUTPUT_DEFAULT_DATE_STRFTIME = "%Y%m%d"
INPUT_FILENAME = "JournalEntries.csv"

# Exact ``JournalEntries.csv`` header names (same contract as default journal export).
REQUIRED_INPUT_COLUMNS = [
    "Transaction Date",
    "Journal Entry Id",
    "Account Number",
    "Account Name",
    "Amount",
    "Posting Type",
    "Currency",
    "Product Type",
    "Product Code",
    "MarketID Finance",
    "Customer Name",
    "Description",
]

REQUIRED_CONFIG_KEYS = ["input_path"]
REQUIRED_FLATTENED_CONFIG_KEYS = [
    "input_path",
    "sftp_host",
    "sftp_username",
    "sftp_remote_path",
]

SFTP_CONNECT_TIMEOUT = 60
SFTP_BANNER_TIMEOUT = 60
SFTP_AUTH_TIMEOUT = 60

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
