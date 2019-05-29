import pytest
import numpy as np
import pandas as pd

import common.transform as transform


def gen_df(start, end):
    year = np.arange(start, end, 1)
    return pd.DataFrame({"value": "some_value", "date": year})


def test_historic():
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
    df_a = gen_df(2000, 2022)
    df_b = gen_df(2002, 2018)
    df_c = gen_df(2008, 2033)
    df_d = gen_df(1999, 2017)
    df_e = gen_df(1999, 2033)
    results = transform.status(df_a, df_b, df_c, df_d, df_e)
    assert any([result["date"].values.tolist() == [2016] for result in results])


def test_status_no_overlap():
    df_a = gen_df(2010, 2015)
    df_b = gen_df(2002, 2008)
    with pytest.raises(ValueError):
        transform.status(df_a, df_b)
