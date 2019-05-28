from functools import reduce
import numpy as np

from common import util


def historic(*dfs, date="date"):
    uniques = [df[date].unique() for df in dfs]
    reduced = reduce(lambda a, b: np.intersect1d(a, b), uniques)
    return [df[df[date].isin(reduced)] for df in dfs]


def status(*dfs, date="date"):
    years = None
    for df in dfs:
        year_set = set(df[date])
        if not years:
            years = year_set
        else:
            years = years.intersection(year_set)

    if len(years) == 0:
        raise ValueError("No overlapping years in dataframes")

    maxDate = max(years)
    return [df[df[date] == maxDate] for df in dfs]


def add_district_id(org, district_column=None):
    df = org.copy()
    df["district"] = df["delbydelid"].str.slice(4, 6)
    if district_column:
        df.loc[df["district"].isnull(), "district"] = df[df["district"].isnull()][
            district_column
        ].apply(util.get_district_id)
        return df
    else:
        return df[df["district"].str.len() > 0]
