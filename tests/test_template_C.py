import operator

from common.output import Output, Metadata
from common.templates import TemplateC
from tests.template_helper import (
    with_ratios,
    without_ratios,
    values_structure,
    count_values,
    test_df,
)
from tests.transform_output_test_data import output_list

template = TemplateC()


def test_standards():
    with_ratios(template, flatten=True)
    without_ratios(template, flatten=True)
    values_structure(template, value_type=list)
    count_values(template, 1)


def test_df_to_template_c():
    data_points = ["d1", "d2"]
    input_df = test_df
    output = Output(
        values=data_points, df=input_df, metadata=Metadata("", []), template=template
    ).generate_output()

    expected_0101 = {
        "id": "0101",
        "geography": "Lodalen",
        "avgRow": False,
        "totalRow": False,
        "values": [
            [
                {"value": "d1_0101_2017", "ratio": "d1_0101_2017_ratio", "date": 2017},
                {"value": "d1_0101_2018", "ratio": "d1_0101_2018_ratio", "date": 2018},
            ],
            [
                {"value": "d2_0101_2017", "date": 2017},
                {"value": "d2_0101_2018", "date": 2018},
            ],
        ],
    }

    expected_01 = {
        "id": "01",
        "geography": "Bydel Gamle Oslo",
        "avgRow": True,
        "totalRow": False,
        "values": [
            [
                {"value": "d1_01_2017", "ratio": "d1_01_2017_ratio", "date": 2017},
                {"value": "d1_01_2018", "ratio": "d1_01_2018_ratio", "date": 2018},
            ],
            [
                {"value": "d2_01_2017", "date": 2017},
                {"value": "d2_01_2018", "date": 2018},
            ],
        ],
    }

    expected_00 = {
        "id": "00",
        "geography": "Oslo i alt",
        "avgRow": False,
        "totalRow": True,
        "values": [
            [
                {"value": "d1_00_2017", "ratio": "d1_00_2017_ratio", "date": 2017},
                {"value": "d1_00_2018", "ratio": "d1_00_2018_ratio", "date": 2018},
            ],
            [
                {"value": "d2_00_2017", "date": 2017},
                {"value": "d2_00_2018", "date": 2018},
            ],
        ],
    }

    file_01_data = next(obj for obj in output if obj["id"] == "01")["data"]

    sub_district_0101 = next(data for data in file_01_data if data["id"] == "0101")
    district_01 = next(data for data in file_01_data if data["id"] == "01")
    oslo = next(data for data in file_01_data if data["id"] == "00")

    assert sub_district_0101 == expected_0101
    assert district_01 == expected_01
    assert oslo == expected_00


def test_output_list():
    data_points = ["d1", "d2"]
    input_df = test_df
    print(input_df)
    output = Output(
        values=data_points, df=input_df, metadata=Metadata("", []), template=template
    ).generate_output()

    output = sorted(output, key=operator.itemgetter("id"))
    output = list(
        map(lambda x: sorted(x["data"], key=operator.itemgetter("id")), output)
    )

    expected = sorted(output_list, key=operator.itemgetter("id"))
    expected = list(
        map(lambda x: sorted(x["data"], key=operator.itemgetter("id")), expected)
    )

    assert output == expected
