from pyspark.sql import SparkSession
from test_config import BRONZE, VOLUME_BASE

CUSTOMERS_REQUIRED_COLS = {"Customer ID", "Customer Name", "email", "Segment", "Country", "City", "State", "Region"}


def spark():
    return SparkSession.getActiveSession()


def test_customers_xlsx_readable():
    import pandas as pd
    df = pd.read_excel(f"{VOLUME_BASE}/Customer.xlsx")
    assert CUSTOMERS_REQUIRED_COLS.issubset(set(df.columns))


def test_customers_table_exists():
    assert "customers" in [t.name for t in spark().catalog.listTables(BRONZE)]


def test_customers_row_count_not_zero():
    assert spark().table(f"{BRONZE}.customers").count() != 0
