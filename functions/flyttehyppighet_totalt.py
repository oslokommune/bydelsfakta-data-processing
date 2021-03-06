import pandas as pd
import numpy as np
from aws_xray_sdk.core import patch_all, xray_recorder
from dataplatform.awslambda.logging import logging_wrapper

import common.transform as transform
import common.aws as common_aws
from common.aggregateV2 import Aggregate, ColumnNames
from common.output import Output, Metadata
from common.templates import Template
from common.event import event_handler

patch_all()

graph_metadata = Metadata(
    heading="Flytting etter alder", series=[{"heading": "Flytting", "subheading": ""}]
)

aldersgruppe_col = "aldersgruppe_5_aar"


@logging_wrapper("flyttehyppighet_totalt")
@xray_recorder.capture("event_handler")
@event_handler(
    flytting_fra_raw="flytting-fra-etter-alder",
    flytting_til_raw="flytting-til-etter-alder",
)
def start(flytting_fra_raw, flytting_til_raw, output_prefix, type_of_ds):
    input_df = generate_input_df(flytting_fra_raw, flytting_til_raw)

    output_list = []
    if type_of_ds == "historisk":
        output_list = output_historic(input_df)
    elif type_of_ds == "status":
        output_list = output_status(input_df)

    if not output_list:
        raise Exception("No data in outputlist")

    common_aws.write_to_intermediate(output_key=output_prefix, output_list=output_list)


def generate_input_df(flytting_fra_raw, flytting_til_raw):
    flytting_df = pd.merge(
        flytting_fra_raw,
        flytting_til_raw,
        on=["date", "bydel_navn", "bydel_id", "aldersgruppe_5_aar"],
    )

    flytting_df["delbydel_id"] = np.nan
    flytting_df["delbydel_navn"] = np.nan

    flytting_df = flytting_df.astype({"delbydel_id": object, "delbydel_navn": object})

    input_df = (
        Aggregate("sum")
        .aggregate(flytting_df, extra_groupby_columns=[aldersgruppe_col])
        .astype({"date": int})
    )
    return input_df


def output_historic(input_df):
    [input_df] = transform.historic(input_df)
    output = Output(
        values=None, df=input_df, metadata=graph_metadata, template=HistoricTemplate()
    ).generate_output()

    return output


def output_status(input_df):
    [input_df] = transform.status(input_df)
    output = Output(
        values=None, df=input_df, metadata=graph_metadata, template=StatusTemplate()
    ).generate_output()
    return output


def _immigration_object(row_data):
    return {
        "alder": row_data[aldersgruppe_col],
        "mellomBydeler": row_data["innflytting_mellom_bydeler"],
        "innenforBydelen": row_data["innflytting_innenfor_bydelen"],
        "tilFraOslo": row_data["innflytting_til_oslo"],
    }


def _emigration_object(row_data):
    return {
        "alder": row_data[aldersgruppe_col],
        "mellomBydeler": row_data["utflytting_mellom_bydeler"],
        "innenforBydelen": row_data["utflytting_innenfor_bydelen"],
        "tilFraOslo": row_data["utflytting_fra_oslo"],
    }


def _value_list(value_collection):
    if value_collection.empty:
        return []
    return value_collection.tolist()


def _values(df):
    value_list = []
    for date, group_df in df.groupby(by=["date"]):
        immigration_list = group_df.apply(lambda row: _immigration_object(row), axis=1)
        emigration_list = group_df.apply(lambda row: _emigration_object(row), axis=1)
        value = {
            "year": date,
            "immigration": _value_list(immigration_list),
            "emigration": _value_list(emigration_list),
        }
        value_list.append(value)

    return value_list


class HistoricTemplate(Template):
    def values(self, df, series, column_names=ColumnNames()):
        return _values(df)


class StatusTemplate(Template):
    def values(self, df, series, column_names=ColumnNames()):
        return _values(df).pop()
