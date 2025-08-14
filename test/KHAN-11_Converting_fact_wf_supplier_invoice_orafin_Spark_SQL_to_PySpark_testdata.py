spark.catalog.setCurrentCatalog("purgo_databricks")

# PySpark test data generation for purgo_playground.f_supplier_invoice
# All necessary imports
from pyspark.sql import SparkSession  
from pyspark.sql.types import (StructType, StructField, StringType, TimestampType, LongType)  
from pyspark.sql import Row  
from datetime import datetime  

spark = SparkSession.builder.getOrCreate()

# Define schema for f_supplier_invoice (Unity Catalog: purgo_databricks.purgo_playground.f_supplier_invoice)
f_supplier_invoice_schema = StructType([
    StructField("invc_entry_period", TimestampType(), True),  # Invoice entry period in YYYYMM format
    StructField("suplr_invc_nbr", StringType(), True),        # Supplier invoice number
    StructField("vchr_nbr", StringType(), True),              # Voucher number
    StructField("vchr_line_nbr", StringType(), True),         # Voucher line number
    StructField("fscl_yr_nbr", LongType(), True),             # Fiscal year number
    StructField("vchr_type_cd", StringType(), True),          # Voucher type code
    StructField("vchr_status", StringType(), True),           # Voucher status
    StructField("supplier_cd", StringType(), True),           # Supplier code
    StructField("supplier_name", StringType(), True),         # Supplier name
    StructField("supplier_type_cd", StringType(), True),      # Supplier type code
    StructField("suplr_invc_dt", TimestampType(), True),      # Supplier invoice date
    StructField("ap_payment_term_cd", StringType(), True),    # AP payment term code
    StructField("ap_payment_term_desc", StringType(), True),  # AP payment term description
    StructField("cost_centre_cd", StringType(), True),        # Cost centre code
    StructField("cost_centre_nm", StringType(), True),        # Cost centre name
    StructField("gl_acct_id", StringType(), True),            # GL account ID
    StructField("gl_acct_nm", StringType(), True),            # GL account name
    StructField("inv_line_desc", StringType(), True),         # Invoice line description
    StructField("remit_to_addr_line_1", StringType(), True),  # Remit to address line 1
    StructField("column1", StringType(), True),               # Custom column 1
    StructField("column2", StringType(), True),               # Custom column 2
    StructField("column3", StringType(), True),               # Custom column 3
])

# Helper function to parse Databricks timestamp format
def parse_dbx_ts(ts_str):
    # Databricks timestamp format: '2024-03-21T00:00:00.000+0000'
    try:
        return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%f%z")
    except Exception:
        return None

# Generate diverse test records
test_records = [
    # Happy path: all valid fields
    Row(
        invc_entry_period=parse_dbx_ts("2024-03-21T00:00:00.000+0000"),
        suplr_invc_nbr="INV-10001",
        vchr_nbr="VCHR-20001",
        vchr_line_nbr="1",
        fscl_yr_nbr=2024,
        vchr_type_cd="REG",
        vchr_status="APPROVED",
        supplier_cd="SUP-001",
        supplier_name="Supplier A",
        supplier_type_cd="Type1",
        suplr_invc_dt=parse_dbx_ts("2024-03-22T00:00:00.000+0000"),
        ap_payment_term_cd="NET30",
        ap_payment_term_desc="Net 30 Days",
        cost_centre_cd="CC100",
        cost_centre_nm="Cost Centre Alpha",
        gl_acct_id="4000-100-01",
        gl_acct_nm="GL Account Main",
        inv_line_desc="Purchase of office supplies",
        remit_to_addr_line_1="123 Main St",
        column1="Extra1",
        column2="Extra2",
        column3="Extra3"
    ),
    # Edge case: NULLs in optional fields
    Row(
        invc_entry_period=parse_dbx_ts("2023-12-31T23:59:59.999+0000"),
        suplr_invc_nbr=None,
        vchr_nbr="VCHR-20002",
        vchr_line_nbr="2",
        fscl_yr_nbr=2023,
        vchr_type_cd="ADJ",
        vchr_status=None,
        supplier_cd="SUP-002",
        supplier_name=None,
        supplier_type_cd=None,
        suplr_invc_dt=None,
        ap_payment_term_cd=None,
        ap_payment_term_desc=None,
        cost_centre_cd=None,
        cost_centre_nm=None,
        gl_acct_id=None,
        gl_acct_nm=None,
        inv_line_desc=None,
        remit_to_addr_line_1=None,
        column1=None,
        column2=None,
        column3=None
    ),
    # Error case: out-of-range fiscal year
    Row(
        invc_entry_period=parse_dbx_ts("2025-01-01T00:00:00.000+0000"),
        suplr_invc_nbr="INV-10003",
        vchr_nbr="VCHR-20003",
        vchr_line_nbr="3",
        fscl_yr_nbr=9999,  # Out-of-range
        vchr_type_cd="REG",
        vchr_status="PENDING",
        supplier_cd="SUP-003",
        supplier_name="Supplier C",
        supplier_type_cd="Type2",
        suplr_invc_dt=parse_dbx_ts("2025-01-02T00:00:00.000+0000"),
        ap_payment_term_cd="NET60",
        ap_payment_term_desc="Net 60 Days",
        cost_centre_cd="CC200",
        cost_centre_nm="Cost Centre Beta",
        gl_acct_id="5000-200-02",
        gl_acct_nm="GL Account Secondary",
        inv_line_desc="Purchase of IT equipment",
        remit_to_addr_line_1="456 Tech Ave",
        column1="Extra4",
        column2="Extra5",
        column3="Extra6"
    ),
    # Error case: invalid combination (approved but missing supplier)
    Row(
        invc_entry_period=parse_dbx_ts("2022-06-15T12:00:00.000+0000"),
        suplr_invc_nbr="INV-10004",
        vchr_nbr="VCHR-20004",
        vchr_line_nbr="4",
        fscl_yr_nbr=2022,
        vchr_type_cd="REG",
        vchr_status="APPROVED",
        supplier_cd=None,  # Invalid: approved but no supplier
        supplier_name=None,
        supplier_type_cd=None,
        suplr_invc_dt=parse_dbx_ts("2022-06-16T00:00:00.000+0000"),
        ap_payment_term_cd="NET15",
        ap_payment_term_desc="Net 15 Days",
        cost_centre_cd="CC300",
        cost_centre_nm="Cost Centre Gamma",
        gl_acct_id="6000-300-03",
        gl_acct_nm="GL Account Tertiary",
        inv_line_desc="Purchase of lab equipment",
        remit_to_addr_line_1="789 Science Rd",
        column1="Extra7",
        column2="Extra8",
        column3="Extra9"
    ),
    # Edge case: special characters and multi-byte characters
    Row(
        invc_entry_period=parse_dbx_ts("2024-04-01T00:00:00.000+0000"),
        suplr_invc_nbr="INV-特殊字符-10005",
        vchr_nbr="VCHR-20005",
        vchr_line_nbr="5",
        fscl_yr_nbr=2024,
        vchr_type_cd="特殊类型",
        vchr_status="已批准",
        supplier_cd="SUP-特殊",
        supplier_name="供应商Ω",
        supplier_type_cd="类型β",
        suplr_invc_dt=parse_dbx_ts("2024-04-02T00:00:00.000+0000"),
        ap_payment_term_cd="即付",
        ap_payment_term_desc="立即付款",
        cost_centre_cd="CC特殊",
        cost_centre_nm="成本中心Δ",
        gl_acct_id="7000-特殊-04",
        gl_acct_nm="GL账户Σ",
        inv_line_desc="购买特殊设备",
        remit_to_addr_line_1="地址一号",
        column1="额外字段1",
        column2="额外字段2",
        column3="额外字段3"
    ),
    # Edge case: empty string values
    Row(
        invc_entry_period=parse_dbx_ts("2024-05-01T00:00:00.000+0000"),
        suplr_invc_nbr="",
        vchr_nbr="",
        vchr_line_nbr="",
        fscl_yr_nbr=2024,
        vchr_type_cd="",
        vchr_status="",
        supplier_cd="",
        supplier_name="",
        supplier_type_cd="",
        suplr_invc_dt=parse_dbx_ts("2024-05-02T00:00:00.000+0000"),
        ap_payment_term_cd="",
        ap_payment_term_desc="",
        cost_centre_cd="",
        cost_centre_nm="",
        gl_acct_id="",
        gl_acct_nm="",
        inv_line_desc="",
        remit_to_addr_line_1="",
        column1="",
        column2="",
        column3=""
    ),
    # Happy path: valid, with long strings
    Row(
        invc_entry_period=parse_dbx_ts("2024-06-01T00:00:00.000+0000"),
        suplr_invc_nbr="INV-" + "A"*50,
        vchr_nbr="VCHR-" + "B"*50,
        vchr_line_nbr="6",
        fscl_yr_nbr=2024,
        vchr_type_cd="REGULAR",
        vchr_status="APPROVED",
        supplier_cd="SUP-" + "C"*50,
        supplier_name="Supplier " + "D"*50,
        supplier_type_cd="Type" + "E"*50,
        suplr_invc_dt=parse_dbx_ts("2024-06-02T00:00:00.000+0000"),
        ap_payment_term_cd="NET90",
        ap_payment_term_desc="Net 90 Days",
        cost_centre_cd="CC400",
        cost_centre_nm="Cost Centre Delta",
        gl_acct_id="8000-400-05",
        gl_acct_nm="GL Account Quaternary",
        inv_line_desc="Purchase of consulting services",
        remit_to_addr_line_1="101 Consulting Blvd",
        column1="Extra10",
        column2="Extra11",
        column3="Extra12"
    ),
    # Error case: invalid timestamp format
    Row(
        invc_entry_period=parse_dbx_ts("invalid-timestamp"),
        suplr_invc_nbr="INV-10007",
        vchr_nbr="VCHR-20007",
        vchr_line_nbr="7",
        fscl_yr_nbr=2024,
        vchr_type_cd="REG",
        vchr_status="REJECTED",
        supplier_cd="SUP-007",
        supplier_name="Supplier G",
        supplier_type_cd="Type3",
        suplr_invc_dt=parse_dbx_ts("2024-07-02T00:00:00.000+0000"),
        ap_payment_term_cd="NET45",
        ap_payment_term_desc="Net 45 Days",
        cost_centre_cd="CC500",
        cost_centre_nm="Cost Centre Epsilon",
        gl_acct_id="9000-500-06",
        gl_acct_nm="GL Account Fifth",
        inv_line_desc="Purchase of rejected items",
        remit_to_addr_line_1="202 Rejection St",
        column1="Extra13",
        column2="Extra14",
        column3="Extra15"
    ),
    # Edge case: boundary fiscal year (minimum)
    Row(
        invc_entry_period=parse_dbx_ts("2000-01-01T00:00:00.000+0000"),
        suplr_invc_nbr="INV-10008",
        vchr_nbr="VCHR-20008",
        vchr_line_nbr="8",
        fscl_yr_nbr=2000,
        vchr_type_cd="REG",
        vchr_status="APPROVED",
        supplier_cd="SUP-008",
        supplier_name="Supplier H",
        supplier_type_cd="Type4",
        suplr_invc_dt=parse_dbx_ts("2000-01-02T00:00:00.000+0000"),
        ap_payment_term_cd="NET10",
        ap_payment_term_desc="Net 10 Days",
        cost_centre_cd="CC600",
        cost_centre_nm="Cost Centre Zeta",
        gl_acct_id="1000-600-07",
        gl_acct_nm="GL Account Sixth",
        inv_line_desc="Purchase of legacy items",
        remit_to_addr_line_1="303 Legacy Ave",
        column1="Extra16",
        column2="Extra17",
        column3="Extra18"
    ),
    # Edge case: boundary fiscal year (maximum)
    Row(
        invc_entry_period=parse_dbx_ts("2099-12-31T23:59:59.999+0000"),
        suplr_invc_nbr="INV-10009",
        vchr_nbr="VCHR-20009",
        vchr_line_nbr="9",
        fscl_yr_nbr=2099,
        vchr_type_cd="REG",
        vchr_status="APPROVED",
        supplier_cd="SUP-009",
        supplier_name="Supplier I",
        supplier_type_cd="Type5",
        suplr_invc_dt=parse_dbx_ts("2099-12-31T23:59:59.999+0000"),
        ap_payment_term_cd="NET120",
        ap_payment_term_desc="Net 120 Days",
        cost_centre_cd="CC700",
        cost_centre_nm="Cost Centre Eta",
        gl_acct_id="1100-700-08",
        gl_acct_nm="GL Account Seventh",
        inv_line_desc="Purchase of future items",
        remit_to_addr_line_1="404 Future Rd",
        column1="Extra19",
        column2="Extra20",
        column3="Extra21"
    ),
    # Error case: special characters in cost centre code
    Row(
        invc_entry_period=parse_dbx_ts("2024-08-01T00:00:00.000+0000"),
        suplr_invc_nbr="INV-10010",
        vchr_nbr="VCHR-20010",
        vchr_line_nbr="10",
        fscl_yr_nbr=2024,
        vchr_type_cd="REG",
        vchr_status="APPROVED",
        supplier_cd="SUP-010",
        supplier_name="Supplier J",
        supplier_type_cd="Type6",
        suplr_invc_dt=parse_dbx_ts("2024-08-02T00:00:00.000+0000"),
        ap_payment_term_cd="NET15",
        ap_payment_term_desc="Net 15 Days",
        cost_centre_cd="CC!@#$%^&*()",
        cost_centre_nm="Cost Centre Special",
        gl_acct_id="1200-800-09",
        gl_acct_nm="GL Account Eighth",
        inv_line_desc="Purchase of special items",
        remit_to_addr_line_1="505 Special St",
        column1="Extra22",
        column2="Extra23",
        column3="Extra24"
    ),
    # Happy path: valid, with multi-byte characters in supplier name
    Row(
        invc_entry_period=parse_dbx_ts("2024-09-01T00:00:00.000+0000"),
        suplr_invc_nbr="INV-10011",
        vchr_nbr="VCHR-20011",
        vchr_line_nbr="11",
        fscl_yr_nbr=2024,
        vchr_type_cd="REG",
        vchr_status="APPROVED",
        supplier_cd="SUP-011",
        supplier_name="供应商🧬",
        supplier_type_cd="Type7",
        suplr_invc_dt=parse_dbx_ts("2024-09-02T00:00:00.000+0000"),
        ap_payment_term_cd="NET30",
        ap_payment_term_desc="Net 30 Days",
        cost_centre_cd="CC900",
        cost_centre_nm="Cost Centre Theta",
        gl_acct_id="1300-900-10",
        gl_acct_nm="GL Account Ninth",
        inv_line_desc="Purchase of biotech items",
        remit_to_addr_line_1="606 Biotech Blvd",
        column1="Extra25",
        column2="Extra26",
        column3="Extra27"
    ),
    # Edge case: NULL in all fields
    Row(
        invc_entry_period=None,
        suplr_invc_nbr=None,
        vchr_nbr=None,
        vchr_line_nbr=None,
        fscl_yr_nbr=None,
        vchr_type_cd=None,
        vchr_status=None,
        supplier_cd=None,
        supplier_name=None,
        supplier_type_cd=None,
        suplr_invc_dt=None,
        ap_payment_term_cd=None,
        ap_payment_term_desc=None,
        cost_centre_cd=None,
        cost_centre_nm=None,
        gl_acct_id=None,
        gl_acct_nm=None,
        inv_line_desc=None,
        remit_to_addr_line_1=None,
        column1=None,
        column2=None,
        column3=None
    ),
    # Happy path: valid, with all fields filled
    Row(
        invc_entry_period=parse_dbx_ts("2024-10-01T00:00:00.000+0000"),
        suplr_invc_nbr="INV-10012",
        vchr_nbr="VCHR-20012",
        vchr_line_nbr="12",
        fscl_yr_nbr=2024,
        vchr_type_cd="REG",
        vchr_status="APPROVED",
        supplier_cd="SUP-012",
        supplier_name="Supplier L",
        supplier_type_cd="Type8",
        suplr_invc_dt=parse_dbx_ts("2024-10-02T00:00:00.000+0000"),
        ap_payment_term_cd="NET60",
        ap_payment_term_desc="Net 60 Days",
        cost_centre_cd="CC1000",
        cost_centre_nm="Cost Centre Iota",
        gl_acct_id="1400-1000-11",
        gl_acct_nm="GL Account Tenth",
        inv_line_desc="Purchase of consulting",
        remit_to_addr_line_1="707 Consulting St",
        column1="Extra28",
        column2="Extra29",
        column3="Extra30"
    ),
    # Error case: fiscal year as string (should be cast to LongType)
    Row(
        invc_entry_period=parse_dbx_ts("2024-11-01T00:00:00.000+0000"),
        suplr_invc_nbr="INV-10013",
        vchr_nbr="VCHR-20013",
        vchr_line_nbr="13",
        fscl_yr_nbr=int("2024"),  # Cast string to int
        vchr_type_cd="REG",
        vchr_status="APPROVED",
        supplier_cd="SUP-013",
        supplier_name="Supplier M",
        supplier_type_cd="Type9",
        suplr_invc_dt=parse_dbx_ts("2024-11-02T00:00:00.000+0000"),
        ap_payment_term_cd="NET30",
        ap_payment_term_desc="Net 30 Days",
        cost_centre_cd="CC1100",
        cost_centre_nm="Cost Centre Kappa",
        gl_acct_id="1500-1100-12",
        gl_acct_nm="GL Account Eleventh",
        inv_line_desc="Purchase of software",
        remit_to_addr_line_1="808 Software Ave",
        column1="Extra31",
        column2="Extra32",
        column3="Extra33"
    ),
    # Edge case: very large voucher line number
    Row(
        invc_entry_period=parse_dbx_ts("2024-12-01T00:00:00.000+0000"),
        suplr_invc_nbr="INV-10014",
        vchr_nbr="VCHR-20014",
        vchr_line_nbr="999999999999",
        fscl_yr_nbr=2024,
        vchr_type_cd="REG",
        vchr_status="APPROVED",
        supplier_cd="SUP-014",
        supplier_name="Supplier N",
        supplier_type_cd="Type10",
        suplr_invc_dt=parse_dbx_ts("2024-12-02T00:00:00.000+0000"),
        ap_payment_term_cd="NET15",
        ap_payment_term_desc="Net 15 Days",
        cost_centre_cd="CC1200",
        cost_centre_nm="Cost Centre Lambda",
        gl_acct_id="1600-1200-13",
        gl_acct_nm="GL Account Twelfth",
        inv_line_desc="Purchase of hardware",
        remit_to_addr_line_1="909 Hardware Rd",
        column1="Extra34",
        column2="Extra35",
        column3="Extra36"
    ),
    # Error case: invalid supplier code (empty string)
    Row(
        invc_entry_period=parse_dbx_ts("2025-01-01T00:00:00.000+0000"),
        suplr_invc_nbr="INV-10015",
        vchr_nbr="VCHR-20015",
        vchr_line_nbr="15",
        fscl_yr_nbr=2025,
        vchr_type_cd="REG",
        vchr_status="APPROVED",
        supplier_cd="",
        supplier_name="Supplier O",
        supplier_type_cd="Type11",
        suplr_invc_dt=parse_dbx_ts("2025-01-02T00:00:00.000+0000"),
        ap_payment_term_cd="NET30",
        ap_payment_term_desc="Net 30 Days",
        cost_centre_cd="CC1300",
        cost_centre_nm="Cost Centre Mu",
        gl_acct_id="1700-1300-14",
        gl_acct_nm="GL Account Thirteenth",
        inv_line_desc="Purchase of invalid supplier",
        remit_to_addr_line_1="1010 Invalid St",
        column1="Extra37",
        column2="Extra38",
        column3="Extra39"
    ),
    # Edge case: multi-byte in custom columns
    Row(
        invc_entry_period=parse_dbx_ts("2025-02-01T00:00:00.000+0000"),
        suplr_invc_nbr="INV-10016",
        vchr_nbr="VCHR-20016",
        vchr_line_nbr="16",
        fscl_yr_nbr=2025,
        vchr_type_cd="REG",
        vchr_status="APPROVED",
        supplier_cd="SUP-016",
        supplier_name="Supplier P",
        supplier_type_cd="Type12",
        suplr_invc_dt=parse_dbx_ts("2025-02-02T00:00:00.000+0000"),
        ap_payment_term_cd="NET60",
        ap_payment_term_desc="Net 60 Days",
        cost_centre_cd="CC1400",
        cost_centre_nm="Cost Centre Nu",
        gl_acct_id="1800-1400-15",
        gl_acct_nm="GL Account Fourteenth",
        inv_line_desc="Purchase of multi-byte custom",
        remit_to_addr_line_1="1111 Multi-byte Rd",
        column1="字段一",
        column2="字段二",
        column3="字段三"
    ),
    # Happy path: valid, with all fields filled and special characters in GL account name
    Row(
        invc_entry_period=parse_dbx_ts("2025-03-01T00:00:00.000+0000"),
        suplr_invc_nbr="INV-10017",
        vchr_nbr="VCHR-20017",
        vchr_line_nbr="17",
        fscl_yr_nbr=2025,
        vchr_type_cd="REG",
        vchr_status="APPROVED",
        supplier_cd="SUP-017",
        supplier_name="Supplier Q",
        supplier_type_cd="Type13",
        suplr_invc_dt=parse_dbx_ts("2025-03-02T00:00:00.000+0000"),
        ap_payment_term_cd="NET90",
        ap_payment_term_desc="Net 90 Days",
        cost_centre_cd="CC1500",
        cost_centre_nm="Cost Centre Xi",
        gl_acct_id="1900-1500-16",
        gl_acct_nm="GL Account !@#$%^&*()",
        inv_line_desc="Purchase of special GL",
        remit_to_addr_line_1="1212 Special GL St",
        column1="Extra40",
        column2="Extra41",
        column3="Extra42"
    ),
    # Error case: missing required fields (voucher number)
    Row(
        invc_entry_period=parse_dbx_ts("2025-04-01T00:00:00.000+0000"),
        suplr_invc_nbr="INV-10018",
        vchr_nbr=None,
        vchr_line_nbr="18",
        fscl_yr_nbr=2025,
        vchr_type_cd="REG",
        vchr_status="APPROVED",
        supplier_cd="SUP-018",
        supplier_name="Supplier R",
        supplier_type_cd="Type14",
        suplr_invc_dt=parse_dbx_ts("2025-04-02T00:00:00.000+0000"),
        ap_payment_term_cd="NET30",
        ap_payment_term_desc="Net 30 Days",
        cost_centre_cd="CC1600",
        cost_centre_nm="Cost Centre Omicron",
        gl_acct_id="2000-1600-17",
        gl_acct_nm="GL Account Fifteenth",
        inv_line_desc="Purchase of missing voucher",
        remit_to_addr_line_1="1313 Missing VCHR St",
        column1="Extra43",
        column2="Extra44",
        column3="Extra45"
    ),
    # Edge case: very long custom column values
    Row(
        invc_entry_period=parse_dbx_ts("2025-05-01T00:00:00.000+0000"),
        suplr_invc_nbr="INV-10019",
        vchr_nbr="VCHR-20019",
        vchr_line_nbr="19",
        fscl_yr_nbr=2025,
        vchr_type_cd="REG",
        vchr_status="APPROVED",
        supplier_cd="SUP-019",
        supplier_name="Supplier S",
        supplier_type_cd="Type15",
        suplr_invc_dt=parse_dbx_ts("2025-05-02T00:00:00.000+0000"),
        ap_payment_term_cd="NET60",
        ap_payment_term_desc="Net 60 Days",
        cost_centre_cd="CC1700",
        cost_centre_nm="Cost Centre Pi",
        gl_acct_id="2100-1700-18",
        gl_acct_nm="GL Account Sixteenth",
        inv_line_desc="Purchase of long custom",
        remit_to_addr_line_1="1414 Long Custom St",
        column1="X"*100,
        column2="Y"*100,
        column3="Z"*100
    ),
    # Happy path: valid, with all fields filled and edge fiscal year
    Row(
        invc_entry_period=parse_dbx_ts("2025-06-01T00:00:00.000+0000"),
        suplr_invc_nbr="INV-10020",
        vchr_nbr="VCHR-20020",
        vchr_line_nbr="20",
        fscl_yr_nbr=2025,
        vchr_type_cd="REG",
        vchr_status="APPROVED",
        supplier_cd="SUP-020",
        supplier_name="Supplier T",
        supplier_type_cd="Type16",
        suplr_invc_dt=parse_dbx_ts("2025-06-02T00:00:00.000+0000"),
        ap_payment_term_cd="NET120",
        ap_payment_term_desc="Net 120 Days",
        cost_centre_cd="CC1800",
        cost_centre_nm="Cost Centre Rho",
        gl_acct_id="2200-1800-19",
        gl_acct_nm="GL Account Seventeenth",
        inv_line_desc="Purchase of edge fiscal year",
        remit_to_addr_line_1="1515 Edge Fiscal St",
        column1="Extra46",
        column2="Extra47",
        column3="Extra48"
    ),
]

# Create DataFrame
df_f_supplier_invoice = spark.createDataFrame(test_records, schema=f_supplier_invoice_schema)

# Show the test data
df_f_supplier_invoice.show(truncate=False)
