import sys
import pandas as pd
import numpy as np
import unittest
import datasets_for_testing
sys.path.insert(0, r'..\common')  # Needed to import the module to be tested
import aggregate_dfs
import datasets_for_testing

# Local copies of the test data in datasets_for_testing
df_org = datasets_for_testing.data_sets['df2_org'].copy()
df_agg_districts = datasets_for_testing.data_sets['df2_agg_districts'].copy()
df_agg_Oslo = datasets_for_testing.data_sets['df2_agg_Oslo'].copy()
df_agg_total = datasets_for_testing.data_sets['df2_agg_total'].copy()


def _sort_dfs(dfs):

    """
    Internal method to sort the DataFrame by row and column to make sure two DataFrames which have the same
    content in different order can be compared.
    """

    def _sort_df(df):
        basic_cols = ['date', 'delbydelid', 'district']
        used_basic_cols = [col for col in basic_cols if col in list(df.columns)]
        other_cols = sorted([col for col in list(df.columns) if col not in basic_cols])
        col_order = [*used_basic_cols, *other_cols]
        df = df[col_order]
        sort_order = [col for col in ['delbydelid', 'district', 'date'] if col in used_basic_cols]
        df = df.sort_values(by=sort_order).reset_index(drop=True)

        return df

    return [_sort_df(df) for df in dfs]


class Tester(unittest.TestCase):

    # Other assert functions are available at
    # https://docs.python.org/3/library/unittest.html#unittest.TestCase

    def test__wmean(self):

        aggregation = {'agg_func': 'wmean',
                       'data_points': 'mean_income',
                       'data_weights': 'inhabitants'}
        groupby = ['date']
        df_act = aggregate_dfs._wmean(df_org, aggregation, groupby)
        df_act, df_exp = _sort_dfs([df_act, df_agg_Oslo.copy()])

        for col in df_act.columns:
            self.assertListEqual(list(df_act[col]), list(df_exp[col]))

    def test__one_aggregation(self):

        aggregation = {'agg_func': 'sum',
                       'data_points': 'inhabitants'}
        groupby = ['date']
        df_act = aggregate_dfs._one_aggregation(df_org, aggregation, groupby)
        df_act, df_exp = _sort_dfs([df_act, df_agg_Oslo.copy()])

        for col in df_act.columns:
            self.assertListEqual(list(df_act[col]), list(df_exp[col]))

    def test__aggregate_district(self):

        DISTRICT = '01'

        aggregations = [{'agg_func': 'sum',
                         'data_points': 'inhabitants'},
                        {'agg_func': 'wmean',
                         'data_points': 'mean_income',
                         'data_weights': 'inhabitants'}]

        df_act = aggregate_dfs._aggregate_district(df_org, DISTRICT, aggregations)
        df_act, df_exp = _sort_dfs([df_act, df_agg_districts[df_agg_districts['district'] == DISTRICT].copy()])

        for col in df_act.columns:
            self.assertListEqual(list(df_act[col]), list(df_exp[col]))

    def test_aggregate_from_subdistricts(self):

        aggregations = [{'agg_func': 'sum',
                         'data_points': 'inhabitants'},
                        {'agg_func': 'wmean',
                         'data_points': 'mean_income',
                         'data_weights': 'inhabitants'}]

        df_act = aggregate_dfs.aggregate_from_subdistricts(df_org, aggregations)
        df_act, df_exp = _sort_dfs([df_act, df_agg_total.copy()])

        for col in df_act.columns:
            # In self.assertListEqual nan!=nan.
            # Because of this test only on
            if col == 'delbydelid':
                mask = df_act['delbydelid'].notnull()
                self.assertListEqual(list(df_act[mask][col]), list(df_exp[mask][col]))
            else:
                self.assertListEqual(list(df_act[col]), list(df_exp[col]))

    def test_aggregate_from_subdistricts_2(self):

        aggregations = [{'agg_func': 'sum',
                         'data_points': ['inhabitants']},
                        {'agg_func': 'wmean',
                         'data_points': 'mean_income',
                         'data_weights': 'inhabitants'}]

        with self.assertRaises(ValueError):
            aggregate_dfs.aggregate_from_subdistricts(df_org, aggregations)

    def test_merge_df(self):

        df1 = df_org[['date', 'district', 'delbydelid', 'mean_income']].copy()
        df2 = df_org[['date', 'district', 'delbydelid', 'inhabitants']].copy()

        df_act = aggregate_dfs.merge_dfs(df1, df2)

        df_exp = df_org.copy()
        df_act, df_exp = _sort_dfs([df_act, df_exp])

        for col in df_act.columns:
            self.assertListEqual(list(df_act[col]), list(df_exp[col]))

    def test_add_ratios(self):

        df = df_org.copy()

        df['double_mean_income'] = df['mean_income'] * 2

        df = aggregate_dfs.add_ratios(df, ['mean_income', 'double_mean_income'], ['mean_income', 'double_mean_income'])

        self.assertListEqual([1/3 for i in range(len(df))], list(df['mean_income_ratio']))
        self.assertListEqual([2/3 for i in range(len(df))], list(df['double_mean_income_ratio']))


if __name__ == '__main__':

    unittest.main()
