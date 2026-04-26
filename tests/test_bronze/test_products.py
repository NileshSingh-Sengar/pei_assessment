from pyspark.sql import SparkSession
from test_config import BRONZE, VOLUME_BASE

PRODUCTS_REQUIRED_COLS = {"Product ID", "Category", "Sub-Category", "Product Name"}


def spark():
    return SparkSession.getActiveSession()


def test_products_csv_readable():
    df = spark().read.option("header", "true").csv(f"{VOLUME_BASE}/Products.csv")
    assert df.count() > 0


def test_products_csv_has_required_columns():
    df = spark().read.option("header", "true").csv(f"{VOLUME_BASE}/Products.csv")
    assert PRODUCTS_REQUIRED_COLS.issubset(set(df.columns))


def test_products_table_exists():
    assert "products" in [t.name for t in spark().catalog.listTables(BRONZE)]


def test_products_row_count_not_zero():
    assert spark().table(f"{BRONZE}.products").count() != 0
