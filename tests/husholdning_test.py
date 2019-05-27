import unittest
import pandas as pd

df_with_district_id = pd.read_csv(
    f"tests/husholdning_test_input.csv",
    sep=";",
    converters={"delbydelid": lambda x: str(x), "district": lambda x: str(x)},
).rename(columns={"År": "date"})


class Tester(unittest.TestCase):
    def noop(self):
        """
        TODO: Add new test when using the correct input format.
        :return:
        """
        pass


if __name__ == "__main__":
    unittest.main()
