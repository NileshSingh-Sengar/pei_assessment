from pyspark.sql import SparkSession, functions as F
from test_config import SILVER

DIM_CUSTOMER_COLS = {
    "customer_id", "customer_name", "email", "phone", "address",
    "segment", "country", "city", "state", "postal_code", "region"
}


def spark():
    return SparkSession.getActiveSession()


def normalize_col_name(name: str) -> str:
    return name.strip().lower().replace(" ", "_").replace("-", "_")


def test_normalize_customer_id():
    assert normalize_col_name("Customer ID") == "customer_id"


def test_normalize_postal_code():
    assert normalize_col_name("Postal Code") == "postal_code"


def test_dim_customer_table_exists():
    assert "dim_customer" in [t.name for t in spark().catalog.listTables(SILVER)]


def test_dim_customer_has_expected_columns():
    df = spark().table(f"{SILVER}.dim_customer")
    assert DIM_CUSTOMER_COLS.issubset(set(df.columns))


def test_dim_customer_columns_are_snake_case():
    df = spark().table(f"{SILVER}.dim_customer")
    for col in df.columns:
        assert col == col.lower() and " " not in col and "-" not in col


def test_dim_customer_non_null():
    assert spark().table(f"{SILVER}.dim_customer").count() != 0


def test_dim_customer_no_duplicate_customer_id():
    df = spark().table(f"{SILVER}.dim_customer")
    assert df.count() == df.select("customer_id").distinct().count()


def test_dim_customer_no_null_customer_name():
    df = spark().table(f"{SILVER}.dim_customer")
    assert df.filter(df["customer_name"].isNull() | (df["customer_name"] == "")).count() == 0


def test_dim_customer_no_digits_in_name():
    df = spark().table(f"{SILVER}.dim_customer")
    assert df.filter(df["customer_name"].rlike(r"\d")).count() == 0


def test_dim_customer_no_special_chars_in_name():
    df = spark().table(f"{SILVER}.dim_customer")
    assert df.filter(df["customer_name"].rlike(r"[^\p{L}\s'\-]")).count() == 0


def test_dim_customer_no_trailing_leading_hyphens_in_name():
    df = spark().table(f"{SILVER}.dim_customer")
    assert df.filter(df["customer_name"].rlike(r"^\-|\-$")).count() == 0


def test_dim_customer_no_multiple_spaces_in_name():
    df = spark().table(f"{SILVER}.dim_customer")
    assert df.filter(df["customer_name"].rlike(r"\s{2,}")).count() == 0


def test_dim_customer_no_leading_trailing_whitespace():
    df = spark().table(f"{SILVER}.dim_customer")
    assert df.filter(df["customer_name"] != F.trim(df["customer_name"])).count() == 0


def test_dim_customer_customer_id_uppercase():
    df = spark().table(f"{SILVER}.dim_customer")
    assert df.filter(df["customer_id"] != F.upper(df["customer_id"])).count() == 0


def test_dim_customer_no_error_phone():
    df = spark().table(f"{SILVER}.dim_customer")
    assert df.filter(df["phone"] == "#ERROR!").count() == 0


def test_dim_customer_no_negative_phone():
    df = spark().table(f"{SILVER}.dim_customer")
    assert df.filter(df["phone"].rlike(r"^-\d+$")).count() == 0


def test_dim_customer_quarantine_table_exists():
    assert "dim_customer_quarantine" in [t.name for t in spark().catalog.listTables(SILVER)]