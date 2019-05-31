import numpy as np
import pandas as pd
import pytest
from functions.folkemengde import (
    filter_10year_set,
    calculate_change,
    calculate_change_ratio,
)


@pytest.fixture
def df():
    years = range(2008, 2020)
    d1 = [f"d1_{year}" for year in years]
    return pd.DataFrame(data={"date": years, "d1": d1})


class TestFilter10YearSet:
    def test_valid(self, df):
        df = filter_10year_set(df)
        assert df.to_dict("records") == [
            {"date": 2009, "d1": "d1_2009"},
            {"date": 2019, "d1": "d1_2019"},
        ]

    def test_out_of_bounds(self, df):
        df = df[df.date > 2013]
        with pytest.raises(ValueError):
            filter_10year_set(df)


class TestCalculateChange:
    def test_change(self):
        df = pd.DataFrame(
            data={
                "date": [2009, 2010],
                "population": [100, 105],
                "bydel_id": "01",
                "delbydel_id": "0101",
            }
        )
        df = calculate_change(df, column_name="change")

        assert len(df) == 2
        assert np.isnan(df.iloc[0].change)
        assert df.iloc[1].change == 5

    def test_single_year(self):
        df = pd.DataFrame(
            data={
                "date": [2009],
                "population": [100],
                "bydel_id": "01",
                "delbydel_id": "0101",
            }
        )
        df = calculate_change(df, column_name="change")

        assert len(df) == 1
        assert np.isnan(df.iloc[0].change)


class TestCalculateChangeRatio:
    def test_change_ratio(self, df):
        df["population"] = range(10, len(df) + 10)
        df["change"] = [np.nan, *[1] * 11]
        df["change_10y"] = [*[np.nan] * 11, 10]
        df = calculate_change_ratio(df)

        assert (df[df.date == 2009].change_ratio == 0.1).all()
        assert (df[df.date == 2019].change_ratio == 0.05).all()
        assert df[df.date < 2019].change_10y_ratio.isna().all()
        assert np.isclose(df[df.date == 2019].change_10y_ratio, 0.909_091).all()

    def test_missing_years(self, df):
        df = df[df.date > 2016].copy()
        df["population"] = range(10, len(df) + 10)
        df["change"] = [np.nan, 1, 1]
        df["change_10y"] = [np.nan, np.nan, 1]
        df = calculate_change_ratio(df)

        assert (df[df.date == 2017].change_ratio.isna()).all()
        assert np.isclose(df[df.date == 2019].change_ratio, 0.090_909).all()
        assert df.change_10y_ratio.isna().all()
