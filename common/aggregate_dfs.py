import sys
import os
import pandas as pd
import numpy as np

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def _check_data_point_validity(df, data_points):

    """
    Simple test - are all the data_points present as DataFrame columns.
    """

    for dp in data_points:
        if dp not in list(df.columns):
            raise ValueError('{dp} is not a column in df.'.format(dp=dp))


def _check_data_consistency(df):

    """
    Not an immediate critical function, but we need to discussed possible usage.
    This function checks if the data set is complete with entries for all years for all sub_districts.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        None
    """

    COLS = ['sub_district', 'date']

    count_dates = df[COLS].groupby(by=['sub_district']).agg('count').reset_index(drop=False)
    count_sub_districts = df[COLS].groupby(by=['date']).agg('count').reset_index(drop=False)

    print(count_dates)
    print(count_sub_districts)


def aggregate_from_subdistricts(df, data_points, agg_func='sum'):

    """
    This function aggregates values from lower-level districts into higher-level districts.
    In reality: sub_districts --> districts --> Oslo_total

    TBD: 'mean' can be done, but this is not 'weighted mean', which is what we need

    Args:
        df (pd.DataFrame): A DataFrame
        data_points (list): The column names of the columns to be aggregated.
        agg_func (str): A valid aggregate function. Default is 'sum', (unweighted) 'mean' can also be used.

    Returns:
        df_agg (pd.DataFrame): The original DataFrame with additional rows for districts and Oslo total.

    Raise:
        ValueError: One or more of arguments are not as expected.
    """

    # Simple validity check
    _check_data_point_validity(df, data_points)
    if agg_func not in ['sum', 'mean']:
        raise ValueError('{af} is not a valid aggregation function.'.format(af=agg_func))

    # Some initialization
    df_no_agg = df[df['sub_district'].notnull()].copy()  # Remove pre-existing aggregations in the DataFrame
    df_agg = df_no_agg.copy()  # Then start to add aggregations to this DataFrame
    all_districts = list(df['district'].unique())

    # Aggregate districts from sub_districts
    for district in all_districts:

        # Aggregation
        df_district = df_no_agg[df_no_agg['district'] == district].copy()
        district_agg = df_district[['district', 'date', *data_points]].groupby(by=['date'],
                                                                                 as_index=False).agg(agg_func)
        district_agg['sub_district'] = np.nan
        district_agg['district'] = district

        # Concatenate aggregation to main DataFrame
        df_agg = pd.concat((df_agg, district_agg), axis=0, sort=False).reset_index(drop=True)

    # Aggregate Oslo in total from sub_districts
    # Decision: "Marka", "Sentrum" and "ikke registrert" should be included in Oslo total, according to Niels Henning.
    oslo_agg = df_no_agg[['date', *data_points]].groupby(by=['date'], as_index=False).agg(agg_func)
    oslo_agg['sub_district'] = np.nan
    oslo_agg['district'] = '00'
    df_agg = pd.concat((df_agg, oslo_agg), axis=0, sort=False).reset_index(drop=True)

    return df_agg


def add_ratios(df, data_points):

    """
    This function adds a ratio column for all the data points specified.

    Normally it will be used after aggregate_from_subdistricts.

    Args:
        df (pd.DataFrame): A DataFrame containing the data.
        data_points (list): The column names of the columns to be used in the ratio calculation.

    Returns:
        df_ratios (pd.DataFrame): The original DataFrame with additional columns for ratios.

    Raise:
        ValueError: If the strings in data_points does not match the column names in df.

    """

    _check_data_point_validity(df, data_points)

    df_ratios = df.copy()
    sums = df[data_points].sum(axis=1)

    for dp in data_points:
        col_name = '{dp}_ratio'.format(dp=dp)
        df_ratios[col_name] = df_ratios[dp] / sums

    return df_ratios


def merge_dfs(df1, df2, how='inner', suffixes=['_1', '_2']):

    """
    This function can be used to merge DataFrames, normally, but not necessarily after aggregation.
    It is a requirement that both DataFrames contain the columns 'date', 'district' and 'sub_district'.

    Args:
        df1 (pd.DataFrame): A DataFrame to be merged.
        df2 (pd.DataFrame): A DataFrame to be merged.
        how (str): Join method. Alternatives: 'inner', 'left', 'right', 'outer'.
        suffixes (list of str): Suffixes to be added to new column if there are overlapping column names in df1 and df2.

    Returns:
        df_merged (pd.DataFrame): The original DataFrame with additional columns for ratios.

    Raise:
        ValueError: If there is found anything unexpected with the arguments passed.
    """

    # Input check - DataFrames
    for df in [df1, df2]:
        for col in ['date', 'district', 'sub_district']:
            if col not in df.columns:
                print(df.columns)
                raise ValueError('Column {col} is not found in the DataFrame header'.format(col=col))
    # Input check - how
    if how not in ['inner', 'left', 'right', 'outer']:
        raise ValueError('The argument how needs to be either "inner", "left", "right" or "outer".')
    # Input check - suffixes
    if len(suffixes) != 2:
        print('Suffixes={s}'.format(s=str(suffixes)))
        raise ValueError('The length of suffixes is {length}, not 2 as expected.'.format(length=len(suffixes)))

    df_merged = pd.merge(df1, df2, on=['date', 'district', 'sub_district'], suffixes=['_1', '_2'])

    return df_merged


if __name__ == '__main__':

    # This section is present to demonstrate functionality.
    # Also check

    import datasets_for_testing

    df = datasets_for_testing.df1
    data_points = ['value_A', 'value_B']
    print('Note that the total will drop in 2018 since district 02 doesn\'t have data for 2018.')
    df['district'] = df['sub_district'].str.slice(4, 6)  # This will be done by a function in the "add_features" module.
    print(df)

    print('Perform aggregation.')
    df = aggregate_from_subdistricts(df, data_points, agg_func='sum')
    print(df)

    print('Add ratios.')
    df = add_ratios(df, data_points)
    print(df)

    TEST_MERGE = False
    if TEST_MERGE:
        print('Make another dataset to do a merge.')
        df2 = df.loc[:7, ['sub_district', 'date']].copy()
        print(df2.shape)

        df2['value_A'] = [500, 500, 500, 500, 400, 400, 400, 400]
        df2['value_C'] = list('ABCDEFGH')
        data_points2 = ['value_A', 'value_C']

        df2['district'] = df2['sub_district'].str.slice(4, 6)
        df2 = aggregate_from_subdistricts(df2, data_points2, agg_func='sum')

        print('Merging it with the following DataFrame...')
        print(df2)

        print('..results in this DataFrame:')
        df_merge = merge_dfs(df, df2)
        print(df_merge)



