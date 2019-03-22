import sys
import pandas as pd
import numpy as np
import unittest
import datasets_for_testing
sys.path.insert(0, r'..\common')  # Needed to import the module to be tested
import aggregate_dfs

df_org = pd.DataFrame({'date': [2017, 2017, 2018, 2018],
                       'sub_district': ['0300010001', '0300020001', '0300010001', '0300020001'],
                       'district': ['01', '02', '01', '02'],
                       'mean_income': [400000, 500000, 420000, 525000],
                       'inhabitants': [2000, 3000, 2200, 3300]})

df_agg_districts = df_org.copy()
df_agg_districts['sub_district'] = [np.nan, np.nan, np.nan, np.nan]

df_agg_Oslo = pd.DataFrame({'date': [2017, 2018],
                            'sub_district': [np.nan, np.nan],
                            'district': ['00', '00'],
                            'mean_income': [460000.0, 483000.0],
                            'inhabitants': [5000, 5500]})

df_agg_total = pd.concat((df_org, df_agg_districts, df_agg_Oslo), axis=0, sort=True)


def _sort_dfs(dfs):

    """
    Internal method to sort the DataFrame by row and column to make sure two DataFrames which have the same
    content in different order can be compared.
    """

    def _sort_df(df):
        basic_cols = ['date', 'sub_district', 'district']
        used_basic_cols = [col for col in basic_cols if col in list(df.columns)]
        other_cols = sorted([col for col in list(df.columns) if col not in basic_cols])
        col_order = [*used_basic_cols, *other_cols]
        df = df[col_order]
        sort_order = [col for col in ['sub_district', 'district', 'date'] if col in used_basic_cols]
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
            if col == 'sub_district':
                mask = df_act['sub_district'].notnull()
                self.assertListEqual(list(df_act[mask][col]), list(df_exp[mask][col]))
            else:
                self.assertListEqual(list(df_act[col]), list(df_exp[col]))

    def test_merge_df(self):

        df1 = df_org[['date', 'district', 'sub_district', 'mean_income']].copy()
        df2 = df_org[['date', 'district', 'sub_district', 'inhabitants']].copy()

        df_act = aggregate_dfs.merge_dfs(df1, df2)

        df_exp = df_org.copy()
        df_act, df_exp = _sort_dfs([df_act, df_exp])

        for col in df_act.columns:
            self.assertListEqual(list(df_act[col]), list(df_exp[col]))


    # TO BE DONE!
    # add_ratios(df, data_points)



if __name__ == '__main__':

    unittest.main()

