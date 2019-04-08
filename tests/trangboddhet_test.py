import unittest
import sys

sys.path.insert(0, r"..")  # Needed to import the module to be tested

import functions.trangboddhet as trangboddhet
import tests.datasets_for_testing as test_data


class Tester(unittest.TestCase):
    def test_trangboddhet(self):

        df_source = test_data.data_sets["df_trangboddhet_org"]

        output_data = trangboddhet.data_processing(df_source)

        # Run a test on each type of template (depending on original to be perfect).
        self.assertEqual(
            output_data["trangboddhet_alle_status"][0]["data"][0]["values"][0]["value"],
            555,
        )
        self.assertEqual(
            output_data["trangboddhet_alle_historisk"][0]["data"][0]["values"][0][0][
                "value"
            ],
            505,
        )
        self.assertEqual(
            output_data["trangboddhet_under0.5_status"][0]["data"][0]["values"][0][
                "value"
            ],
            555,
        )
        self.assertEqual(
            output_data["trangboddhet_under0.5_historisk"][0]["data"][0]["values"][0][
                "value"
            ],
            505,
        )


if __name__ == "__main__":
    unittest.main()
