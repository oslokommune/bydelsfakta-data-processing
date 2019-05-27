import unittest

import pandas as pd

from common.aggregateV2 import ColumnNames, Aggregate
from tests import datasets_for_testing

column_names = ColumnNames()


def _sort_dfs(dfs):

    """
    Internal method to sort the DataFrame by row and column to make sure two DataFrames which have the same
    content in different order can be compared.
    """

    def _sort_df(df):
        basic_cols = [
            column_names.date,
            column_names.sub_district_id,
            column_names.district_id,
        ]
        used_basic_cols = [col for col in basic_cols if col in list(df.columns)]
        other_cols = sorted([col for col in list(df.columns) if col not in basic_cols])
        col_order = [*used_basic_cols, *other_cols]
        df = df[col_order]
        sort_order = [
            col
            for col in [
                column_names.date,
                column_names.sub_district_id,
                column_names.district_id,
            ]
            if col in used_basic_cols
        ]
        df = df.sort_values(by=sort_order).reset_index(drop=True)

        return df

    return [_sort_df(df) for df in dfs]


class Tester(unittest.TestCase):

    # Other assert functions are available at
    # https://docs.python.org/3/library/unittest.html#unittest.TestCase

    def test__aggregation_sum(self):
        hus = datasets_for_testing.husholdinger.content()

        aggregation = {"personer_1": "sum"}
        agg = Aggregate(aggregation)
        df_act = agg.aggregate(hus)

        expected_oslo_i_alt = (
            hus.groupby([column_names.date])["personer_1"].sum().reset_index()
        )
        expected_oslo_i_alt["bydel_id"] = "00"

        expected_districts = (
            hus.groupby([column_names.date, column_names.district_id])["personer_1"]
            .sum()
            .reset_index()
        )

        expected_sub_districts = (
            hus.groupby(
                [
                    column_names.date,
                    column_names.district_id,
                    column_names.sub_district_id,
                ]
            )["personer_1"]
            .sum()
            .reset_index()
        )

        expected = expected_sub_districts.append(expected_districts)
        expected = expected.append(expected_oslo_i_alt)
        assert list(df_act["personer_1"].sort_values()) == list(
            expected["personer_1"].sort_values()
        )

    def test_merge_df(self):

        df1 = datasets_for_testing.husholdinger.content()[
            [
                column_names.date,
                column_names.sub_district_id,
                column_names.district_id,
                "personer_1",
                "personer_4",
            ]
        ]
        df2 = datasets_for_testing.husholdinger.content()[
            [
                column_names.date,
                column_names.sub_district_id,
                column_names.district_id,
                "personer_2",
            ]
        ]
        df_expected = datasets_for_testing.husholdinger.content()[
            [
                column_names.date,
                column_names.sub_district_id,
                column_names.district_id,
                "personer_1",
                "personer_2",
                "personer_4",
            ]
        ]

        df_act = Aggregate({}).merge(df1, df2)
        df_act, df_exp = _sort_dfs([df_act, df_expected])

        for col in df_act.columns:
            self.assertListEqual(list(df_act[col]), list(df_exp[col]))

    def test_add_ratios(self):

        df = datasets_for_testing.husholdinger.content()

        df["ratio_1_2"] = df["personer_1"] / df["personer_2"]

        df = Aggregate({}).add_ratios(
            df, data_points=["personer_1"], ratio_of=["personer_2"]
        )

        assert list(df["ratio_1_2"].sort_values()) == list(
            df["personer_1_ratio"].sort_values()
        )

    def test_duplication_is_handled(self):
        df = datasets_for_testing.husholdinger.content()
        df = pd.concat((df, df.iloc[0:1, :]), axis=0).reset_index(drop=True)

        aggregation = {"personer_1": "sum"}
        agg = Aggregate(aggregation)
        df_act = agg.aggregate(df)

        expected_oslo_i_alt = (
            df.groupby([column_names.date])["personer_1"].sum().reset_index()
        )
        expected_oslo_i_alt["bydel_id"] = "00"

        expected_districts = (
            df.groupby([column_names.date, column_names.district_id])["personer_1"]
            .sum()
            .reset_index()
        )

        expected_sub_districts = (
            df.groupby(
                [
                    column_names.date,
                    column_names.district_id,
                    column_names.sub_district_id,
                ]
            )["personer_1"]
            .sum()
            .reset_index()
        )

        expected = expected_sub_districts.append(expected_districts)
        expected = expected.append(expected_oslo_i_alt)

        expected = list(expected["personer_1"].sort_values())
        result = list(df_act["personer_1"].sort_values())

        for i in range(0, len(result)):
            assert result[i] == expected[i]


if __name__ == "__main__":

    unittest.main()
