
import common
import pandas as pd
import numpy as np


def test_add_district_id_simple():
    delbydelids = np.array(["0301010101", "0301030303", "0301010103", "0301010104", None])
    districts = np.array(["01", "03", "01", "01", None])
    df = pd.DataFrame({'value': "some_value", 'delbydelid': delbydelids})
    df_expected = pd.DataFrame({'value': "some_value", 'delbydelid': delbydelids, 'district': districts})

    result = common._add_district_id(df)
    print(result)
    print(df_expected)
    assert result.equals(df_expected)


def test_historic():
    def gen_df(start, end):
        year = np.arange(start, end, 1)
        return pd.DataFrame({'value': "some_value", 'date': year})
    df_a = gen_df(2000, 2022)
    df_b = gen_df(2002, 2018)
    df_c = gen_df(2008, 2033)
    df_d = gen_df(1999, 2017)
    df_e = gen_df(1999, 2033)
    results = common._historic(df_a, df_b, df_c,df_d,df_e)
    for result in results:
        print(result['date'].values.tolist())
        print(list(range(2008, 2017, 1)))
        assert result['date'].values.tolist() == list(range(2008, 2017, 1))

def test_status():
    def gen_df(start, end):
        year = np.arange(start, end, 1)
        return pd.DataFrame({'value': "some_value", 'date': year})
    df_a = gen_df(2000, 2022)
    df_b = gen_df(2002, 2018)
    df_c = gen_df(2008, 2033)
    df_d = gen_df(1999, 2017)
    df_e = gen_df(1999, 2033)
    results = common._status(df_a, df_b, df_c,df_d,df_e)
    assert any([result['date'].values.tolist() == [2032] for result in results])

