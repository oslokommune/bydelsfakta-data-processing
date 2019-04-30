import unittest
import functions.trangboddhet as trangboddhet
import tests.datasets_for_testing as test_data


class Tester(unittest.TestCase):
    def test_trangboddhet(self):

        df_source = test_data.data_sets["df_trangboddhet_org"]

        output_data = trangboddhet.data_processing(df_source)

        # Run a test on each type of template (depending on original to be perfect).
        self.assertEqual(
            output_data["trangboddhet-alle-status"][0]["data"][0]["values"][0]["value"],
            555,
        )
        self.assertEqual(
            output_data["trangboddhet-alle-historisk"][0]["data"][0]["values"][0][0][
                "value"
            ],
            505,
        )
        self.assertEqual(
            output_data["trangboddhet-under-0-5-status"][0]["data"][0]["values"][0][
                "value"
            ],
            555,
        )
        self.assertEqual(
            output_data["trangboddhet-under-0-5-historisk"][0]["data"][0]["values"][0][
                "value"
            ],
            505,
        )


if __name__ == "__main__":
    unittest.main()
