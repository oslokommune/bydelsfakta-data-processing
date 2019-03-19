from functools import reduce

import numpy as np
import pandas as pd

s3_bucket = "ok-origo-dataplatform-dev"


def read_from_s3(s3_key, value_column, date_column="År", dtypes = None):
    if dtypes is None:
        dtypes = { 'delbydelid': object }
    return pd.read_csv(f's3://{s3_bucket}/{s3_key}', sep=';', dtypes=dtypes).rename(columns={value_column: 'value', date_column: 'date'})


def _historic(*dfs):
    uniques = [df['date'].unique() for df in dfs]
    reduced = reduce(lambda a, b: np.intersect1d(a, b), uniques)
    return [df[df['date'].isin(reduced)] for df in dfs]


def _status(*dfs):
    uniques = [df['date'].max() for df in dfs]
    maxDate = reduce(lambda a, b: max(a,b), uniques)
    return [df[df['date'] == maxDate] for df in dfs]


def _add_district_id(df):
    df['district'] = df['delbydelid'].str.slice(4, 6)
    return df