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


@pytest.fixture
def ratio_df(df):
    df["population"] = range(10, len(df) + 10)
    df["change"] = [np.nan, *[1] * 11]
    df["change_10y"] = [*[np.nan] * 11, 10]
    df["bydel_id"] = "01"
    df["delbydel_id"] = "0101"
    return df


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
    def test_columns_are_dropped(self):
        df = pd.DataFrame(
            data={
                "date": [2009, 2010],
                "population": [100, 105],
                "bydel_id": "01",
                "delbydel_id": "0101",
                "bydel_navn": "Fydel",
                "delbydel_navn": "Delfydel",
            }
        )
        try:
            df = calculate_change(df, column_name="change")
        except Exception:
            pytest.fail("bydel_navn and delbydel_navn columns were not dropped")

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
    def test_change_ratio(self, ratio_df):
        df = calculate_change_ratio(ratio_df)

        assert (df[df.date == 2009].change_ratio == 0.1).all()
        assert (df[df.date == 2019].change_ratio == 0.05).all()
        assert df[df.date < 2019].change_10y_ratio.isna().all()
        assert np.isclose(df[df.date == 2019].change_10y_ratio, 0.909_091).all()

    def test_missing_years(self, ratio_df):
        df = ratio_df[ratio_df.date > 2016].copy()
        df = calculate_change_ratio(df)

        assert (df[df.date == 2017].change_ratio.isna()).all()
        assert np.isclose(df[df.date == 2019].change_ratio, 0.05).all()
        assert df.change_10y_ratio.isna().all()

    def test_sorted_data(self, ratio_df):
        df = ratio_df
        actual = df.iloc[1].change / df.iloc[0].population

        df2 = df.copy()
        df2["bydel_id"] = "01"
        df2["delbydel_id"] = "0102"

        df = pd.concat([df, df2])
        df = df.sort_values("date")
        df = calculate_change_ratio(df)

        assert df.iloc[1].change_ratio == actual
