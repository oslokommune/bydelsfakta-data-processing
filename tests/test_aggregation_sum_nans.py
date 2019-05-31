import numpy as np
import pandas as pd
import pytest
from common.aggregation import sum_nans


class TestSumNans:
    def test_values(self):
        series = pd.Series(data=[4, 5, 6])
        assert sum_nans(series) == 15

    def test_nans(self):
        series = pd.Series(data=[np.nan, np.nan, np.nan])
        assert np.isnan(sum_nans(series))

    def test_mix(self):
        series = pd.Series(data=[4, 5, np.nan, 7])
        with pytest.raises(ValueError):
            sum_nans(series)
