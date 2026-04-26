from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DateType
from test_config import GOLD

FACT_REQUIRED_COLS = {
    "row_id", "order_id", "order_date", "ship_date", "ship_mode",
    "customer_id", "product_id", "quantity", "price", "discount",
    "profit", "profit_rounded", "year",
    "customer_name", "country",
    "category", "sub_category"
}


def spark():
    return SparkSession.getActiveSession()


# --- Pure unit tests (no Spark table needed) ---

def round_profit(value):
    return round(value, 2)


def test_profit_rounded_positive():
    assert round_profit(63.691) == 63.69


def test_profit_rounded_negative():
    assert round_profit(-14.923) == -14.92


def test_profit_rounded_already_two_dp():
    assert round_profit(102.19) == 102.19


def test_profit_rounded_zero():
    assert round_profit(0.0) == 0.0


def test_profit_rounded_large_negative():
    # Data has min profit ~ -16438; validates large losses are not truncated
    assert round_profit(-16438.0) == -16438.0


# --- Integration tests against the live Delta table ---

def test_fact_order_table_exists():
    assert "fact_order" in [t.name for t in spark().catalog.listTables(GOLD)]


def test_fact_order_has_expected_columns():
    df = spark().table(f"{GOLD}.fact_order")
    assert FACT_REQUIRED_COLS.issubset(set(df.columns))


def test_fact_order_not_null():
    assert spark().table(f"{GOLD}.fact_order").count() != 0


def test_fact_order_no_null_order_id():
    df = spark().table(f"{GOLD}.fact_order")
    assert df.filter(df["order_id"].isNull()).count() == 0


def test_fact_order_order_date_is_date_type():
    df = spark().table(f"{GOLD}.fact_order")
    field = next(f for f in df.schema.fields if f.name == "order_date")
    assert isinstance(field.dataType, DateType)


def test_fact_order_ship_date_is_date_type():
    df = spark().table(f"{GOLD}.fact_order")
    field = next(f for f in df.schema.fields if f.name == "ship_date")
    assert isinstance(field.dataType, DateType)


def test_fact_order_no_null_order_date():
    df = spark().table(f"{GOLD}.fact_order")
    assert df.filter(df["order_date"].isNull()).count() == 0


def test_fact_order_no_null_ship_date():
    df = spark().table(f"{GOLD}.fact_order")
    assert df.filter(df["ship_date"].isNull()).count() == 0


def test_fact_order_ship_date_not_before_order_date():
    df = spark().table(f"{GOLD}.fact_order")
    assert df.filter(df["ship_date"] < df["order_date"]).count() == 0

def test_fact_order_year_column_matches_order_date():
    df = spark().table(f"{GOLD}.fact_order")
    assert df.filter(df["year"] != F.year(df["order_date"])).count() == 0


def test_fact_order_profit_rounded_matches_profit():
    df = spark().table(f"{GOLD}.fact_order")
    assert df.filter(df["profit_rounded"] != F.round(df["profit"], 2)).count() == 0


def test_fact_order_contains_negative_profits():
    # ~1870 loss-making orders in the data — must be preserved, not filtered
    df = spark().table(f"{GOLD}.fact_order")
    assert df.filter(df["profit"] < 0).count() > 0


def test_fact_order_customer_name_joined():
    df = spark().table(f"{GOLD}.fact_order")
    assert df.filter(df["customer_name"].isNotNull()).count() > 0


def test_fact_order_category_joined():
    df = spark().table(f"{GOLD}.fact_order")
    assert df.filter(df["category"].isNotNull()).count() > 0
