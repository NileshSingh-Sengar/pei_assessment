from pyspark.sql import SparkSession, functions as F
from test_config import GOLD

AGG_REQUIRED_COLS = {
    "year", "category", "sub_category", "customer_id", "customer_name", "total_profit"
}


def spark():
    return SparkSession.getActiveSession()


def test_aggregate_profit_table_exists():
    assert "aggregate_profit" in [t.name for t in spark().catalog.listTables(GOLD)]


def test_aggregate_profit_has_expected_columns():
    df = spark().table(f"{GOLD}.aggregate_profit")
    assert AGG_REQUIRED_COLS.issubset(set(df.columns))


def test_aggregate_profit_no_duplicate_grain():
    df = spark().table(f"{GOLD}.aggregate_profit")
    total = df.count()
    distinct = df.select("year", "category", "sub_category", "customer_id").distinct().count()
    assert total == distinct


def test_aggregate_profit_row_count_positive():
    assert spark().table(f"{GOLD}.aggregate_profit").count() > 0


def test_aggregate_profit_total_matches_fact():
    # Both sides sum raw profit and round once — consistent with 04_aggregate_profit.py
    fact_total = (
        spark().table(f"{GOLD}.fact_order")
        .agg(F.round(F.sum("profit"), 2))
        .collect()[0][0]
    )
    agg_total = (
        spark().table(f"{GOLD}.aggregate_profit")
        .agg(F.round(F.sum("total_profit"), 2))
        .collect()[0][0]
    )
    assert fact_total == agg_total


def test_aggregate_profit_contains_negative_totals():
    # Some customers had net losses — verify they are present, not filtered
    df = spark().table(f"{GOLD}.aggregate_profit")
    assert df.filter(df["total_profit"] < 0).count() > 0
