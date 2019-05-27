import unittest
import pandas as pd
import common.population_utils as population_utils

population_raw = pd.read_csv(f'test_data/population_utils/population_utils_input.csv', sep=';').rename(
    columns={'aar': 'date'})


class Tester(unittest.TestCase):

    def test_population_total(self):
        population_total = population_utils.generate_population_df(population_raw)
        expected = pd.read_csv(f'test_data/population_utils/population_total_expected.csv', sep=';')
        self.assertCountEqual(
            population_total.to_dict("r"),
            expected.to_dict("r")
        )

    def test_population_in_range(self):
        population_30_to_59 = population_utils.generate_population_df(population_raw, min_age=30, max_age=59)
        expected = pd.read_csv(f'test_data/population_utils/population_30_to_59_expected.csv', sep=';')
        print(population_30_to_59)
        self.assertCountEqual(
            population_30_to_59.to_dict("r"),
            expected.to_dict("r")
        )


if __name__ == "__main__":
    unittest.main()