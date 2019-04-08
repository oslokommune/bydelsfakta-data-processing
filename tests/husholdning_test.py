import unittest
import json
import functions.husholdning as husholdning
import tests.husholdning_test_data as test_data
import pandas as pd

df_with_district_id = pd.read_csv(
    f"tests/husholdning_test_input.csv",
    sep=";",
    converters={"delbydelid": lambda x: str(x), "district": lambda x: str(x)},
).rename(columns={"År": "date"})


class Tester(unittest.TestCase):
    def test_with_data_points(self):
        with_data_points = husholdning.with_household_data_points(df_with_district_id)
        self.assertCountEqual(
            with_data_points.to_dict("r"), test_data.with_data_points_expected
        )

    def test_aggregate_to_input_format(self):
        self.maxDiff = None
        data_points = ["single_adult", "no_children", "with_children"]
        with_data_points = husholdning.with_household_data_points(df_with_district_id)
        input_df = husholdning.aggregate_to_input_format(with_data_points, data_points)

        self.assertEqual(json.dumps(input_df.to_dict("r")), test_data.input_df_expected)


if __name__ == "__main__":
    unittest.main()
