import pandas as pd

import common.transform as transform
import common.aws as common_aws
from common.aggregateV2 import Aggregate, ColumnNames
from common.util import get_latest_edition_of
from common.output import Output, Metadata
from common.templates import Template


graph_metadata = Metadata(
    heading="Flytting", series=[{"heading": "Flytting", "subheading": ""}]
)

flytting_fra_etter_alder_id = "flytting-fra-etter-alder"
flytting_til_etter_alder_id = "flytting-til-etter-alder"

aldersgruppe_col = "aldersgruppe_10_aar"
key_cols = [*ColumnNames().default_groupby_columns(), aldersgruppe_col]


def handle(event, context):
    s3_key_flytting_fra_etter_alder_raw = event["input"][flytting_fra_etter_alder_id]
    s3_key_flytting_til_etter_alder_raw = event["input"][flytting_til_etter_alder_id]

    output_key = event["output"]
    type_of_ds = event["config"]["type"]

    input_df = generate_input_df(
        s3_key_flytting_fra_etter_alder_raw, s3_key_flytting_til_etter_alder_raw
    )

    output_list = []
    if type_of_ds == "historisk":
        output_list = output_historic(input_df)

    elif type_of_ds == "status":
        output_list = output_status(input_df)

    if output_list:
        common_aws.write_to_intermediate(output_key=output_key, output_list=output_list)
        return f"Created {output_key}"

    else:
        raise Exception("No data in outputlist")


def generate_input_df(s3_key_flytting_fra_raw, s3_key_flytting_til_raw):
    flytting_fra_raw = common_aws.read_from_s3(
        s3_key=s3_key_flytting_fra_raw, date_column="aar"
    )
    flytting_til_raw = common_aws.read_from_s3(
        s3_key=s3_key_flytting_til_raw, date_column="aar"
    )

    flytting_df = pd.merge(flytting_fra_raw, flytting_til_raw, on=key_cols)

    value_columns = list(set(flytting_df.columns) - set(key_cols))
    agg = _agg(value_columns)
    input_df = Aggregate(agg).aggregate(
        flytting_df, extra_groupby_columns=[aldersgruppe_col]
    )
    return input_df


def _agg(values):
    agg = {}
    for value in values:
        agg[value] = "sum"
    return agg


def output_historic(input_df):
    [input_df] = transform.historic(input_df)
    output = Output(
        values=None, df=input_df, metadata=graph_metadata, template=CustomTemplate()
    ).generate_output()

    return output


def output_status(input_df):
    [input_df] = transform.status(input_df)
    output = Output(
        values=None, df=input_df, metadata=graph_metadata, template=CustomTemplate()
    ).generate_output()
    return output


class CustomTemplate(Template):
    def _immigration_object(self, row_data):
        return {
            "alder": row_data[aldersgruppe_col],
            "mellomDelbydeler": row_data["innflytting_mellom_delbydeler"],
            "innenforDelbydelen": row_data["innflytting_innenfor_delbydelen"],
            "tilFraOslo": row_data["innflytting_til_oslo"],
        }

    def _emigration_object(self, row_data):
        return {
            "alder": row_data[aldersgruppe_col],
            "mellomDelbydeler": row_data["utflytting_mellom_delbydeler"],
            "innenforDelbydelen": row_data["utflytting_innenfor_delbydelen"],
            "tilFraOslo": row_data["utflytting_fra_oslo"],
        }

    def _value_list(self, value_collection):
        if value_collection.empty:
            return []
        return value_collection.tolist()

    def values(self, df, series, column_names=ColumnNames()):
        value_list = []
        for date, group_df in df.groupby(by=["date"]):
            immigration_list = group_df.apply(
                lambda row: self._immigration_object(row), axis=1
            )
            emigration_list = group_df.apply(
                lambda row: self._emigration_object(row), axis=1
            )
            value = {
                "year": date,
                "immigration": self._value_list(immigration_list),
                "emigration": self._value_list(emigration_list),
            }
            value_list.append(value)

        return value_list


if __name__ == "__main__":
    flytting_fra_etter_alder_s3_key = get_latest_edition_of(flytting_fra_etter_alder_id)
    flytting_til_etter_alder_s3_key = get_latest_edition_of(flytting_til_etter_alder_id)
    handle(
        {
            "input": {
                flytting_fra_etter_alder_id: flytting_fra_etter_alder_s3_key,
                flytting_til_etter_alder_id: flytting_til_etter_alder_s3_key,
            },
            "output": "s3/key/or/prefix",
            "config": {"type": "historisk"},
        },
        None,
    )
