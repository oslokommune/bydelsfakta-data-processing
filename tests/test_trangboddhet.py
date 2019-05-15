import unittest
import functions.trangboddhet as trangboddhet
import tests.datasets_for_testing as test_data


class Tester(unittest.TestCase):
    def test_trangboddhet(self):

        df_source = test_data.data_sets["df_trangboddhet_org"]

        value_labels = [
            "Personer per rom - Under 0,5",
            "Personer per rom - 0,5 - 0,9",
            "Personer per rom - 1,0 - 1,9",
            "Personer per rom - 2,0 og over",
        ]

        output_data = trangboddhet.data_processing(df_source, value_labels)

        # TO BE DONE:
        # The below tests has some bad hard coding which will have to be updated when the meta data handling
        # is updated.

        self.assertEqual(
            output_data["trangboddhet-alle-status"][0]["data"][0]["values"][0]["value"],
            555,
        )

        self.assertEqual(
            output_data["trangboddhet_alle_historisk-4DAEn"][0]["data"][0]["values"][0][
                0
            ]["value"],
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
