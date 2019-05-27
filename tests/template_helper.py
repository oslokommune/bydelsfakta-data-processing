import pandas as pd

from common.output import *

column_names = ColumnNames()
test_df = pd.read_csv(
    f"tests/transform_output_test_input.csv",
    sep=";",
    dtype={column_names.sub_district_id: object, column_names.district_id: object},
)
print(test_df)

test_df_latest = test_df[test_df["date"] == 2018]


def without_ratios(template: Template, flatten=False):
    output_values = template.values(test_df_latest, series=["d2"])
    if flatten:
        output_values = [obj for values_list in output_values for obj in values_list]
    print(output_values)
    assert all(["ratio" not in obj for obj in output_values])
    assert all(["date" in obj for obj in output_values])
    assert all(["value" in obj for obj in output_values])
    return output_values


def with_ratios(template: Template, flatten=False):
    output_values = template.values(test_df_latest, series=["d1"])
    if flatten:
        output_values = [obj for values_list in output_values for obj in values_list]

    print(output_values)
    assert all(["ratio" in obj for obj in output_values])
    assert all(["date" in obj for obj in output_values])
    assert all(["value" in obj for obj in output_values])
    return output_values


def values_structure(template: Template, value_type):
    output_values = template.values(test_df_latest, series=["d1"])
    print(output_values)
    assert type(output_values) == list
    assert all([type(obj) == value_type for obj in output_values])
    return output_values


def count_values(template: Template, expected):
    output_values = template.values(test_df_latest, series=["d1"])
    print(len(output_values))
    assert len(output_values) == expected
    return output_values
