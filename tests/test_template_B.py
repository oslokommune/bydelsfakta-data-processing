from tests.template_helper import *

template = TemplateB()


def test_standards():
    template = TemplateA()
    with_ratios(template)
    without_ratios(template)
    values_structure(template, value_type=dict)
    count_values(template, 8)


def test_df_to_template_b_all():
    input_df = test_df
    data_points = ["d1"]
    output = Output(
        values=data_points, df=input_df, metadata=Metadata("", []), template=template
    ).generate_output()

    expected_0101 = {
        "id": "0101",
        "geography": "Lodalen",
        "avgRow": False,
        "totalRow": False,
        "values": [
            {"value": "d1_0101_2017", "ratio": "d1_0101_2017_ratio", "date": 2017},
            {"value": "d1_0101_2018", "ratio": "d1_0101_2018_ratio", "date": 2018},
        ],
    }

    expected_01 = {
        "id": "01",
        "geography": "Bydel Gamle Oslo",
        "avgRow": True,
        "totalRow": False,
        "values": [
            {"value": "d1_01_2017", "ratio": "d1_01_2017_ratio", "date": 2017},
            {"value": "d1_01_2018", "ratio": "d1_01_2018_ratio", "date": 2018},
        ],
    }

    expected_00 = {
        "id": "00",
        "geography": "Oslo i alt",
        "avgRow": False,
        "totalRow": True,
        "values": [
            {"value": "d1_00_2017", "ratio": "d1_00_2017_ratio", "date": 2017},
            {"value": "d1_00_2018", "ratio": "d1_00_2018_ratio", "date": 2018},
        ],
    }

    file_01_data = next(obj for obj in output if obj[column_names.district_id] == "01")[
        "data"
    ]

    sub_district_0101 = next(data for data in file_01_data if data["id"] == "0101")
    district_01 = next(data for data in file_01_data if data["id"] == "01")
    oslo = next(data for data in file_01_data if data["id"] == "00")

    assert sub_district_0101 == expected_0101
    assert district_01 == expected_01
    assert oslo == expected_00
