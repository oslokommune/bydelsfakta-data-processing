import unittest
import sys

# sys.path.insert(0, r'..\common')  # Needed to import the module to be tested
sys.path.insert(0, r"..")  # Needed to import the module to be tested

import common.transform_output as transform
import tests.transform_output_test_data as test_data
import numpy
import pandas as pd


def empty_str_to_nan(s):
    if len(s) == 0:
        return numpy.nan
    else:
        return s


test_df = pd.read_csv(
    f"tests/transform_output_test_input.csv",
    sep=";",
    converters={
        "delbydelid": lambda x: empty_str_to_nan(str(x)),
        "district": lambda x: str(x),
    },
)

test_df_latest = test_df[test_df["date"] == 2018]


class Tester(unittest.TestCase):
    def test_value_entry(self):
        values_no_ratio = {"d1": 1.0, "date": 2018}
        values_with_ratio = {"d1": 1.0, "d1_ratio": 0.5, "date": 2018}

        self.assertDictEqual(
            transform.value_entry(values_no_ratio, "d1"), {"value": 1.0, "date": 2018}
        )
        self.assertDictEqual(
            transform.value_entry(values_with_ratio, "d1"),
            {"value": 1.0, "ratio": 0.5, "date": 2018},
        )

    def test_list_to_time_series(self):
        data_points = ["d1", "d2", "d3"]
        self.assertDictEqual(
            transform.list_to_time_series(data_points), {"d1": [], "d2": [], "d3": []}
        )

    def test_df_to_template_a(self):
        geography = "0301010101"
        input_df = test_df_latest[test_df_latest["delbydelid"] == geography]
        data_points = ["d1", "d2"]
        output = transform.df_to_template_a(geography, input_df, data_points)
        expected = {
            "geography": "0301010101",
            "linkTo": False,
            "avgRow": False,
            "totalRow": False,
            "values": [
                {"value": "d1_0101_2018", "ratio": "d1_0101_2018_ratio", "date": 2018},
                {"value": "d2_0101_2018", "ratio": "d2_0101_2018_ratio", "date": 2018},
            ],
        }
        self.assertDictEqual(output, expected)

    def test_df_to_template_b(self):
        geography = "0301010101"
        input_df = test_df[test_df["delbydelid"] == geography]
        data_points = ["d1"]
        output = transform.df_to_template_b(geography, input_df, data_points)
        expected = {
            "geography": "0301010101",
            "avgRow": False,
            "totalRow": False,
            "values": [
                {"value": "d1_0101_2017", "ratio": "d1_0101_2017_ratio", "date": 2017},
                {"value": "d1_0101_2018", "ratio": "d1_0101_2018_ratio", "date": 2018},
            ],
        }
        self.assertDictEqual(output, expected)

    def test_df_to_template_b_error(self):
        geography = "0301010101"
        input_df = test_df[test_df["delbydelid"] == geography]
        data_points = ["d1", "d2"]
        with self.assertRaises(Exception):
            transform.df_to_template_b(geography, input_df, data_points)

    def test_df_to_template_c(self):
        geography = "0301010101"
        input_df = test_df[test_df["delbydelid"] == geography]
        data_points = ["d1", "d2"]
        output = transform.df_to_template_c(geography, input_df, data_points)
        expected = {
            "geography": "0301010101",
            "avgRow": False,
            "totalRow": False,
            "values": [
                [
                    {
                        "value": "d1_0101_2017",
                        "ratio": "d1_0101_2017_ratio",
                        "date": 2017,
                    },
                    {
                        "value": "d1_0101_2018",
                        "ratio": "d1_0101_2018_ratio",
                        "date": 2018,
                    },
                ],
                [
                    {
                        "value": "d2_0101_2017",
                        "ratio": "d2_0101_2017_ratio",
                        "date": 2017,
                    },
                    {
                        "value": "d2_0101_2018",
                        "ratio": "d2_0101_2018_ratio",
                        "date": 2018,
                    },
                ],
            ],
        }
        self.assertDictEqual(output, expected)

    def test_df_to_template_i(self):
        geography = "0301010101"
        input_df = test_df_latest[test_df_latest["delbydelid"] == geography]
        data_points = ["d1", "d2"]
        output = transform.df_to_template_i(geography, input_df, data_points)
        expected = {
            "geography": "0301010101",
            "avgRow": False,
            "totalRow": False,
            "values": [
                {"value": "d1_0101_2018", "ratio": "d1_0101_2018_ratio", "date": 2018},
                {"value": "d2_0101_2018", "ratio": "d2_0101_2018_ratio", "date": 2018},
            ],
        }
        self.assertDictEqual(output, expected)

    def test_df_to_template_j(self):
        geography = "0301010101"
        input_df = test_df_latest[test_df_latest["delbydelid"] == geography]
        data_points = ["d1", "d2"]
        output = transform.df_to_template_i(geography, input_df, data_points)
        expected = {
            "geography": "0301010101",
            "avgRow": False,
            "totalRow": False,
            "values": [
                {"value": "d1_0101_2018", "ratio": "d1_0101_2018_ratio", "date": 2018},
                {"value": "d2_0101_2018", "ratio": "d2_0101_2018_ratio", "date": 2018},
            ],
        }
        self.assertDictEqual(output, expected)

    def test_sub_district_time_series(self):
        sub_district = "0301010101"
        template = "c"
        data_points = ["d1", "d2"]
        output = transform.sub_district_time_series(
            test_df, sub_district, template, data_points
        )
        expected = {
            "geography": "0301010101",
            "avgRow": False,
            "totalRow": False,
            "values": [
                [
                    {
                        "value": "d1_0101_2017",
                        "ratio": "d1_0101_2017_ratio",
                        "date": 2017,
                    },
                    {
                        "value": "d1_0101_2018",
                        "ratio": "d1_0101_2018_ratio",
                        "date": 2018,
                    },
                ],
                [
                    {
                        "value": "d2_0101_2017",
                        "ratio": "d2_0101_2017_ratio",
                        "date": 2017,
                    },
                    {
                        "value": "d2_0101_2018",
                        "ratio": "d2_0101_2018_ratio",
                        "date": 2018,
                    },
                ],
            ],
        }
        self.assertDictEqual(output, expected)

    def test_district_time_series(self):
        district = "01"
        template = "c"
        data_points = ["d1", "d2"]
        output = transform.district_time_series(
            test_df, district, template, data_points
        )
        expected = {
            "geography": "01",
            "avgRow": False,
            "totalRow": False,
            "values": [
                [
                    {"value": "d1_01_2017", "ratio": "d1_01_2017_ratio", "date": 2017},
                    {"value": "d1_01_2018", "ratio": "d1_01_2018_ratio", "date": 2018},
                ],
                [
                    {"value": "d2_01_2017", "ratio": "d2_01_2017_ratio", "date": 2017},
                    {"value": "d2_01_2018", "ratio": "d2_01_2018_ratio", "date": 2018},
                ],
            ],
        }
        self.assertDictEqual(output, expected)

    def test_district_time_series_list(self):
        district = "01"
        template = "a"
        data_points = ["d1", "d2"]
        output = transform.district_time_series_list(
            test_df, district, template, data_points
        )
        expected = test_data.district_01_time_series_list
        self.assertListEqual(output, expected)

    def test_generate_output_list(self):
        template = "c"
        data_points = ["d1", "d2"]
        output = transform.generate_output_list(test_df, template, data_points)
        expected = test_data.output_list
        self.assertListEqual(output, expected)


if __name__ == "__main__":
    unittest.main()
