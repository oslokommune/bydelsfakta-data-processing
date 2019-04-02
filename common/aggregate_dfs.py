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

    COLS = ['delbydelid', 'date']

    count_dates = df[COLS].groupby(by=['delbydelid']).agg('count').reset_index(drop=False)
    count_sub_districts = df[COLS].groupby(by=['date']).agg('count').reset_index(drop=False)

    print(count_dates)
    print(count_sub_districts)


def _wmean(df, aggregation, groupby):

    """
    Internal function to calculate weighted mean , for one district or Oslo total.

    Param:
        agg_to (str): Valids ['district', 'Oslo']
    """

    dp_col = aggregation['data_points']
    dw_col = aggregation['data_weights']

    df_tmp = df[[*groupby, dp_col, dw_col]].copy()
    df_tmp['product_col'] = df_tmp[dp_col] * df_tmp[dw_col]
    df_tmp = df_tmp.drop(dp_col, axis=1)
    df_tmp = df_tmp.groupby(by=groupby, as_index=False).sum()
    df_tmp[dp_col] = df_tmp['product_col'] / df_tmp[dw_col]
    df_wmean = df_tmp[[*groupby, dp_col]]

    return df_wmean


def _one_aggregation(df, aggregation, groupby):

    """
    Internal method to do one single aggregation operation.
    """

    if aggregation['agg_func'] == 'wmean':
        one_agg = _wmean(df, aggregation, groupby)
    else:
        one_agg = df.groupby(by=groupby, as_index=False).agg(aggregation['agg_func'])
        one_agg = one_agg[[*groupby, aggregation['data_points']]]

    return one_agg


def _aggregate_district(df, district, aggregations):

    """
    Internal method to aggregate for one district.
    district=='00' is Oslo Total
    district=='/d/d' is a district
    """

    if district == '00':
        groupby = ['date']
    else:
        groupby = ['date', 'district']
        df = df[df['district'] == district].copy()

    df_agg_dist = None

    for a in aggregations:

        one_agg = _one_aggregation(df, a, groupby)

        if df_agg_dist is None:
            df_agg_dist = one_agg
        else:
            df_agg_dist = pd.merge(df_agg_dist, one_agg, on=groupby)

    return df_agg_dist


def aggregate_from_subdistricts(df, aggregations):

    """
    This function aggregates values from lower-level districts into higher-level districts.
    In reality: sub_districts --> districts --> Oslo_total

    The aggregate function can be given as any Numpy aggregate function which can be passed to the .agg method.
    This would typically be 'sum' or 'mean'. In addition, an option 'wmean' is available, which gives the weighted
    average between (sub)districts. E.g. two districts with df['mean_income']==[400000, 500000] and
    df['inhabitants']==[2000, 3000] will give a weighted mean incom of 460000, while normal 'mean' will give 450000.

    The argument aggregations should be given as a list of dictionarys. All dictionaries have to include the agg_func
    and data_points keys. When agg_func=='wmean', a the weights corresponding to the data points have to be provided.

    aggregations = [{'agg_func': 'sum',
                     'data_points': 'inhabitants'},
                    {'agg_func': 'wmean',
                     'data_points': 'mean_income',
                     'data_weights': 'inhabitants'}]

    The example above will aggregate (sum) the number of inhabitants and also their mean income, weighted by the number
    of inhabitants in each (sub)district.

    In the rare cases where you need to do more than one aggregation on a column, this needs to be done by doing the
    aggregations separately and merging the resulting DataFrames.

    Args:
        df (pd.DataFrame): The DataFrame with the relevant data.
        aggregations (list of dicts): Best understood by looking at the example above.

    Returns:
        df_agg (pd.DataFrame): The original DataFrame with additional rows for districts and Oslo total.

    Raise:
        ValueError: One or more of arguments are not as expected.
    """

    # Some validity checking
    for a in aggregations:

        try:
            dummy = a['data_weights']
        except KeyError:
            a['data_weights'] = None

        if type(a['data_points']) is not str:
            raise ValueError('data_points is not a string.')

        if a['agg_func'] == 'wmean' and a['data_weights'] is None:
            raise ValueError('agg_func is "wmean", but you have not specified data_weights.')
        if a['agg_func'] != 'wmean' and a['data_weights'] is not None:
            raise ValueError('agg_func is not "wmean", but you have specified data_weights.')

    exp_data_points = [a['data_points'] for a in aggregations]
    exp_data_weights = [a['data_weights'] for a in aggregations if a['data_weights'] is not None]
    expected_columns = list(set().union(exp_data_points, exp_data_weights))
    _check_data_point_validity(df, expected_columns)

    # Some initialization
    df_no_agg = df[df['delbydelid'].notnull()].copy()  # Remove pre-existing aggregations in the DataFrame
    df_agg = df_no_agg.copy()  # Then start to add aggregations to this DataFrame
    all_districts = list(df['district'].unique())  # This does not include Oslo total as a district.

    # Aggregate districts from sub_districts
    for district in all_districts:

        district_agg = _aggregate_district(df_no_agg, district, aggregations)
        district_agg['delbydelid'] = np.nan

        # Concatenate aggregation to main DataFrame
        df_agg = pd.concat((df_agg, district_agg), axis=0, sort=False).reset_index(drop=True)

    # Aggregate Oslo in total from sub_districts
    # Decision: "Marka", "Sentrum" and "ikke registrert" should be included in Oslo total, according to Niels Henning.
    oslo_agg = _aggregate_district(df_no_agg, '00', aggregations)
    oslo_agg['delbydelid'] = np.nan
    oslo_agg['district'] = '00'
    df_agg = pd.concat((df_agg, oslo_agg), axis=0, sort=False).reset_index(drop=True)

    return df_agg


def add_ratios(df, data_points, ratio_of):

    """
    This function adds a ratio column for all the data points specified.
    The value in each of these data points will be devided by the sum of the fields in ratio_of

    Normally it will be used after aggregate_from_subdistricts.

    Args:
        df (pd.DataFrame): A DataFrame containing the data.
        data_points (list): The column names of the columns to be used in the ratio calculation.
        ratio_of (list): The sum of these data fields will be in the denominator when calculating the ratio (No: 'nevner')

    Returns:
        df_ratios (pd.DataFrame): The original DataFrame with additional columns for ratios.

    Raise:
        ValueError: If the strings in data_points does not match the column names in df.

    """

    _check_data_point_validity(df, data_points)

    df_ratios = df.copy()
    sums = df[ratio_of].sum(axis=1)

    for dp in data_points:
        col_name = '{dp}_ratio'.format(dp=dp)
        df_ratios[col_name] = df_ratios[dp] / sums

    return df_ratios


def merge_dfs(df1, df2, how='inner', suffixes=['_1', '_2']):

    """
    This function can be used to merge DataFrames, normally, but not necessarily after aggregation.
    It is a requirement that both DataFrames contain the columns 'date', 'district' and 'delbydelid'.

    Args:
        df1 (pd.DataFrame): A DataFrame to be merged.
        df2 (pd.DataFrame): A DataFrame to be merged.
        how (str): Join method. Alternatives: 'inner', 'left', 'right', 'outer'.
        suffixes (list of str): Suffixes to be added to new column if there are overlapping column names in df1 and df2.

    Returns:
        df_merged (pd.DataFrame): The resulting DataFrame after the merge.

    Raise:
        ValueError: If there is found anything unexpected with the arguments passed.
    """

    # Input check - DataFrames
    for df in [df1, df2]:
        for col in ['date', 'district', 'delbydelid']:
            if col not in df.columns:
                print(df.columns)
                raise ValueError('Column {col} is not found in the DataFrame header'.format(col=col))
    # Input check - how
    if how not in ['inner', 'left', 'right', 'outer']:
        raise ValueError('The argument how needs to be either "inner", "left", "right" or "outer".')
    # Input check - suffixes
    if len(suffixes) != 2:
        print('Suffixes={s}'.format(s=suffixes))
        raise ValueError('The length of suffixes is {length}, not 2 as expected.'.format(length=len(suffixes)))

    df_merged = pd.merge(df1, df2, how=how, on=['date', 'district', 'delbydelid'], suffixes=['_1', '_2'])

    return df_merged


if __name__ == '__main__':

    # This section is present to demonstrate functionality.

    sys.path.insert(0, r'..\tests')  # Needed to import the module to be tested
    import datasets_for_testing

    df = datasets_for_testing.df1

    print(df)

    AGGS = [{'agg_func': 'sum',
             'data_points': 'value_A'},
            {'agg_func': 'wmean',
             'data_points': 'value_C',
             'data_weights': 'value_D'},
            {'agg_func': 'sum',
             'data_points': 'value_D'}]

    df = aggregate_from_subdistricts(df, AGGS)

    print('Aggreated:')
    print(df)

    print('If we need to do two aggregations of the same column - do twice and merge the resulting tables.')
