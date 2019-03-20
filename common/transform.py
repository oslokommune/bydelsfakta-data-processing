from functools import reduce
import numpy as np


def historic(*dfs):
    uniques = [df['date'].unique() for df in dfs]
    reduced = reduce(lambda a, b: np.intersect1d(a, b), uniques)
    return [df[df['date'].isin(reduced)] for df in dfs]


def status(*dfs):
    uniques = [df['date'].max() for df in dfs]
    maxDate = reduce(lambda a, b: max(a, b), uniques)
    return [df[df['date'] == maxDate] for df in dfs]


def add_district_id(df):
    df['district'] = df['delbydelid'].str.slice(4, 6)
    return df