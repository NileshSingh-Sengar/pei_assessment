from pyspark.sql import SparkSession
from test_config import BRONZE, VOLUME_BASE

ORDERS_REQUIRED_COLS = {"Row ID", "Order ID", "Order Date", "Ship Date", "Ship Mode",
                        "Customer ID", "Product ID", "Quantity", "Price", "Discount", "Profit"}


def spark():
    return SparkSession.getActiveSession()


def test_orders_json_readable():
    df = spark().read.option("multiline", "true").json(f"{VOLUME_BASE}/Orders.json")
    assert df.count() > 0


def test_orders_json_has_required_columns():
    df = spark().read.option("multiline", "true").json(f"{VOLUME_BASE}/Orders.json")
    assert ORDERS_REQUIRED_COLS.issubset(set(df.columns))


def test_orders_table_exists():
    assert "orders" in [t.name for t in spark().catalog.listTables(BRONZE)]


def test_orders_row_count_not_zero():
    assert spark().table(f"{BRONZE}.orders").count() != 0


def test_bronze_orders_no_null_customer_id():
    df = spark().table(f"{BRONZE}.orders")
    assert df.filter(df["Customer ID"].isNull()).count() == 0


def test_bronze_orders_no_null_product_id():
    df = spark().table(f"{BRONZE}.orders")
    assert df.filter(df["Product ID"].isNull()).count() == 0
