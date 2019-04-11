import unittest
import sys

sys.path.insert(0, r'..')  # Needed to import the module to be tested

import functions.ikke_fullfort_vgs as ikke_fullfort_vgs
import tests.datasets_for_testing as test_data


class Tester(unittest.TestCase):

    def test_ikke_fullfort_vgs_test(self):

        df_source = test_data.data_sets['df_ikke_vgs']

        output_data = ikke_fullfort_vgs.data_processing(df_source)

        # Run a test on each type of template (depending on original to be perfect).
        self.assertAlmostEqual(output_data['levekar_vgs_status'][0]['data'][0]['values'][0]['value'], 0.2887, 4)
        self.assertAlmostEqual(output_data['levekar_vgs_status'][0]['data'][1]['values'][0]['value'], 0.2222, 4)


if __name__ == '__main__':
    unittest.main()
