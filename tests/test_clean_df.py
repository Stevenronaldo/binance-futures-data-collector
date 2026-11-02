import pandas as pd
import pytest

from lambda_function import clean_df

SCHEMA = {
    "open_time": ("int64", False),
    "volume": ("float64", False),
    "markPrice": ("float64", True),
}


def test_valid_data_gets_schema_dtypes():
    df = pd.DataFrame({"open_time": ["1", "2"], "volume": ["10.5", "3"], "markPrice": ["1.0", "2.0"]})
    out = clean_df(df, "open_time", SCHEMA)
    assert out["volume"].dtype == "float64"
    assert out["open_time"].dtype == "int64"


def test_duplicate_timestamp_keeps_last_row():
    df = pd.DataFrame({"open_time": [1, 1], "volume": [1.0, 2.0], "markPrice": [1.0, 1.0]})
    out = clean_df(df, "open_time", SCHEMA)
    assert len(out) == 1
    assert out["volume"].iloc[0] == 2.0


def test_missing_column_raises():
    df = pd.DataFrame({"open_time": [1], "volume": [1.0]})
    with pytest.raises(ValueError, match="missing columns"):
        clean_df(df, "open_time", SCHEMA)


def test_null_in_non_nullable_column_raises():
    df = pd.DataFrame({"open_time": [1], "volume": [None], "markPrice": [1.0]})
    with pytest.raises(ValueError, match="not nullable"):
        clean_df(df, "open_time", SCHEMA)