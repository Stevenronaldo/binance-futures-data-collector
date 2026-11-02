from lambda_function import quality_check
import pandas as pd
import pytest

HOUR = 3_600_000
NOW = 100 * HOUR  # a fixed "now", in ms

@pytest.fixture(autouse=True)
def frozen_clock(monkeypatch):
    monkeypatch.setattr("lambda_function.time.time", lambda: NOW / 1000)

def test_no_issues_when_data_is_continuous_and_fresh():
    df = pd.DataFrame({"open_time": [97 * HOUR, 98 * HOUR, 99 * HOUR]})
    issues = quality_check("klines", "BTCUSDT", df, "open_time", HOUR, None)
    assert issues == []

def test_a_gap_in_between_timestamp():
    df = pd.DataFrame({"open_time": [95 * HOUR, 96 * HOUR, 99 * HOUR]})
    issues = quality_check("klines", "BTCUSDT", df, "open_time", HOUR, None)
    assert any("Quality: gap" in m for m in issues)
    assert not any("Quality: stale" in m for m in issues)

def test_gap_of_2_periods_is_flagged():
    df = pd.DataFrame({"open_time": [96 * HOUR, 98 * HOUR, 99 * HOUR]})
    issues = quality_check("klines", "BTCUSDT", df, "open_time", HOUR, None)
    assert sum("Quality: gap" in m for m in issues) == 1
    assert not any("Quality: stale" in m for m in issues)

def test_stale_timestamp():
    df = pd.DataFrame({"open_time": [90 * HOUR, 91 * HOUR, 92 * HOUR]})
    issues = quality_check("klines", "BTCUSDT", df, "open_time", HOUR, None)
    assert not any("Quality: gap" in m for m in issues)
    assert any("Quality: stale" in m for m in issues)

def test_staleness_limit_is_allowed():
    df = pd.DataFrame({"open_time": [96 * HOUR, 97 * HOUR, 98 * HOUR]})
    issues = quality_check("klines", "BTCUSDT", df, "open_time", HOUR, None)
    assert not any("Quality: gap" in m for m in issues)
    assert not any("Quality: stale" in m for m in issues)

def test_staleness_just_past_the_limit_is_stale():
    df = pd.DataFrame({"open_time": [96 * HOUR, 97 * HOUR]})
    issues = quality_check("klines", "BTCUSDT", df, "open_time", HOUR, None)
    assert not any("Quality: gap" in m for m in issues)
    assert any("Quality: stale" in m for m in issues)

def test_no_data():
    df = pd.DataFrame()
    issues = quality_check("klines", "BTCUSDT", df, "open_time", HOUR, None)
    assert any("Quality: no data" in m for m in issues)

def test_watermark_gap():
    df = pd.DataFrame({"open_time": [97 * HOUR, 98 * HOUR, 99 * HOUR]})
    issues = quality_check("klines", "BTCUSDT", df, "open_time", HOUR, 90 * HOUR)
    assert any("Quality: gap" in m for m in issues)
    assert not any("Quality: stale" in m for m in issues)

def test_funding_rate_max_gap_rules():
    df = pd.DataFrame({"fundingTime": [75 * HOUR, 83 * HOUR, 92 * HOUR]})
    issues = quality_check("fundingRate", "BTCUSDT", df, "fundingTime", None, None)
    assert sum("Quality: gap" in m for m in issues) == 1
    assert not any("Quality: stale" in m for m in issues)

def test_funding_rate_staleness_limit_is_allowed():
    df = pd.DataFrame({"fundingTime": [91 * HOUR]})
    issues = quality_check("fundingRate", "BTCUSDT", df, "fundingTime", None, None)
    assert not any("Quality: stale" in m for m in issues)

def test_funding_rate_just_past_the_limit_is_stale():
    df = pd.DataFrame({"fundingTime": [90 * HOUR]})
    issues = quality_check("fundingRate", "BTCUSDT", df, "fundingTime", None, None)
    assert any("Quality: stale" in m for m in issues)

def test_timestamp_jitter():
    df = pd.DataFrame({"open_time": [97 * HOUR, 98 * HOUR - 300_000, 99 * HOUR]})
    issues = quality_check("klines", "BTCUSDT", df, "open_time", HOUR, None)
    assert not any("Quality: gap" in m for m in issues)
