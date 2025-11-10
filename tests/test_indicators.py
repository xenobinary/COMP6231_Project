import pandas as pd
from src.indicators.technical import compute_indicators


def test_compute_indicators_minimal():
    data = {"close": [i for i in range(1, 41)]}
    df = pd.DataFrame(data)
    result = compute_indicators(df)
    assert "z_score" in result
    # z_score of strictly increasing series should be positive
    assert result["z_score"] > 0


def test_compute_indicators_insufficient():
    df = pd.DataFrame({"close": [1, 2, 3]})
    result = compute_indicators(df)
    # Not enough data for rolling metrics -> z_score fallback 0
    assert result.get("z_score", 0.0) == 0.0
