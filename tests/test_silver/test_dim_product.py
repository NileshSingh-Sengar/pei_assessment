from pyspark.sql import SparkSession
from test_config import SILVER

DIM_PRODUCT_COLS = {
    "product_id", "category", "sub_category", "product_name",
    "state", "price_per_product"
}


def spark():
    return SparkSession.getActiveSession()


def normalize_col_name(name: str) -> str:
    return name.strip().lower().replace(" ", "_").replace("-", "_")


def test_normalize_sub_category():
    assert normalize_col_name("Sub-Category") == "sub_category"


def test_normalize_price_per_product():
    assert normalize_col_name("Price per product") == "price_per_product"


def test_dim_product_table_exists():
    assert "dim_product" in [t.name for t in spark().catalog.listTables(SILVER)]


def test_dim_product_has_expected_columns():
    df = spark().table(f"{SILVER}.dim_product")
    assert DIM_PRODUCT_COLS.issubset(set(df.columns))


def test_dim_product_columns_are_snake_case():
    df = spark().table(f"{SILVER}.dim_product")
    for col in df.columns:
        assert col == col.lower() and " " not in col and "-" not in col


def test_dim_product_no_duplicate_product_id():
    df = spark().table(f"{SILVER}.dim_product")
    assert df.count() == df.select("product_id").distinct().count()


def test_dim_product_row_count_positive():
    assert spark().table(f"{SILVER}.dim_product").count() > 0


def test_dim_product_quarantine_table_exists():
    assert "dim_product_quarantine" in [t.name for t in spark().catalog.listTables(SILVER)]


def test_dim_product_quarantine_not_empty():
    # Source data has 33 duplicate product IDs (66 quarantine rows)
    assert spark().table(f"{SILVER}.dim_product_quarantine").count() > 0


def test_dim_product_quarantine_ids_represented_in_dim():
    dim = spark().table(f"{SILVER}.dim_product")
    quarantine = spark().table(f"{SILVER}.dim_product_quarantine")
    quarantine_ids = {r["product_id"] for r in quarantine.select("product_id").distinct().collect()}
    dim_ids = {r["product_id"] for r in dim.select("product_id").collect()}
    assert quarantine_ids.issubset(dim_ids)
