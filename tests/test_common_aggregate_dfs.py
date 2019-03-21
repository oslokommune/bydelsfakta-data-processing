import sys
import unittest
import datasets_for_testing
sys.path.insert(0, r'..\common')  # Needed to import the module to be tested
import aggregate_dfs

class Tester(unittest.TestCase):

    # Other assert functions are available at
    # https://docs.python.org/3/library/unittest.html#unittest.TestCase

    def test_aggregate_from_subdistricts(self):

        df1 = datasets_for_testing.df1
        df1_agg_sum = datasets_for_testing.df1_agg_sum

        df1_do_agg_sum = aggregate_dfs.aggregate_from_subdistricts(df1, ['value_A', 'value_B'])

        for col in df1_agg_sum.columns:
            self.assertListEqual(list(df1_agg_sum[col]), list(df1_do_agg_sum[col]))

    # TO BE DONE!
    # aggregate_from_subdistricts(df, data_points, agg_func='sum')
    # add_ratios(df, data_points)
    # def merge_dfs(df1, df2, how='inner', suffixes=['_1', '_2']):



if __name__ == '__main__':

    unittest.main()

