import unittest
import sys

sys.path.insert(0, r'..')  # Needed to import the module to be tested

import functions.trangboddhet as trangboddhet
import tests.datasets_for_testing as test_data


class Tester(unittest.TestCase):

    def test_trangboddhet(self):

        df_source = test_data.data_sets['df_trangboddhet_org']

        output_data = trangboddhet.data_processing(df_source)

        # import json
        # with open(r'C:\CURRENT FILES\dump_test.json', 'wt', encoding='utf-8') as f:
        #    json.dump(output_data, f, indent=4)

        self.assertTrue(True)


if __name__ == '__main__':
    unittest.main()
