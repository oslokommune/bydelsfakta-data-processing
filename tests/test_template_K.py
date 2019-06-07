import operator
import pandas as pd

from common.output import Output, Metadata
from common.templates import TemplateK
from tests.template_helper import column_names
from tests.transform_output_test_data import output_list_k

template = TemplateK()

test_df = pd.read_csv(
    f"tests/template_k_test_input.csv",
    sep=";",
    dtype={column_names.sub_district_id: object, column_names.district_id: object},
)


def test_df_to_template_k():
    data_points = ["d1", "d2"]
    input_df = test_df
    print(input_df)
    output = Output(
        values=data_points, df=input_df, metadata=Metadata("", []), template=template
    ).generate_output()

    print(output)
    expected_0101 = {
        "id": "0101",
        "geography": "Lodalen",
        "values": [
            {
                "date": 2018,
                "districtRatio": "d1_0101_2018_ratio_district",
                "osloRatio": "d1_0101_2018_ratio_oslo",
            },
            {
                "date": 2018,
                "districtRatio": "d2_0101_2018_ratio_district",
                "osloRatio": "d2_0101_2018_ratio_oslo",
            },
        ],
        "avgRow": False,
        "totalRow": False,
    }

    expected_01 = {
        "id": "01",
        "geography": "Bydel Gamle Oslo",
        "values": [
            {
                "date": 2018,
                "districtRatio": "d1_01_2018_ratio_district",
                "osloRatio": "d1_01_2018_ratio_oslo",
            },
            {
                "date": 2018,
                "districtRatio": "d2_01_2018_ratio_district",
                "osloRatio": "d2_01_2018_ratio_oslo",
            },
        ],
        "avgRow": True,
        "totalRow": False,
    }

    expected_00 = {
        "id": "00",
        "geography": "Oslo i alt",
        "values": [
            {
                "date": 2018,
                "districtRatio": "d1_00_2018_ratio_district",
                "osloRatio": "d1_00_2018_ratio_oslo",
            },
            {
                "date": 2018,
                "districtRatio": "d2_00_2018_ratio_district",
                "osloRatio": "d2_00_2018_ratio_oslo",
            },
        ],
        "avgRow": False,
        "totalRow": True,
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

    expected = sorted(output_list_k, key=operator.itemgetter("id"))
    expected = list(
        map(lambda x: sorted(x["data"], key=operator.itemgetter("id")), expected)
    )

    assert output == expected


test_df_to_template_k()
