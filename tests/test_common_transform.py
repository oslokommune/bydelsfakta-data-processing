import common.transform as transform
import pandas as pd
import numpy as np


def test_historic():
    def gen_df(start, end):
        year = np.arange(start, end, 1)
        return pd.DataFrame({"value": "some_value", "date": year})

    df_a = gen_df(2000, 2022)
    df_b = gen_df(2002, 2018)
    df_c = gen_df(2008, 2033)
    df_d = gen_df(1999, 2017)
    df_e = gen_df(1999, 2033)
    results = transform.historic(df_a, df_b, df_c, df_d, df_e)
    for result in results:
        print(result["date"].values.tolist())
        print(list(range(2008, 2017, 1)))
        assert result["date"].values.tolist() == list(range(2008, 2017, 1))


def test_status():
    def gen_df(start, end):
        year = np.arange(start, end, 1)
        return pd.DataFrame({"value": "some_value", "date": year})

    df_a = gen_df(2000, 2022)
    df_b = gen_df(2002, 2018)
    df_c = gen_df(2008, 2033)
    df_d = gen_df(1999, 2017)
    df_e = gen_df(1999, 2033)
    results = transform.status(df_a, df_b, df_c, df_d, df_e)
    assert any([result["date"].values.tolist() == [2032] for result in results])
