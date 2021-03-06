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
    heading="Flytting med innvandringskategorier",
    series=[{"heading": "Flytting med innvandringskategorier", "subheading": ""}],
)

aldersgruppe_col = "aldersgruppe_5_aar"
key_cols = ["date", "bydel_id", "bydel_navn", aldersgruppe_col]


@logging_wrapper("flyttehyppighet_inn_kat")
@xray_recorder.capture("event_handler")
@event_handler(
    flytting_fra_df="flytting-fra-etter-inn-kat",
    flytting_til_df="flytting-til-etter-inn-kat",
)
def start(flytting_fra_df, flytting_til_df, output_prefix, type_of_ds):
    input_df = generate_input_df(flytting_fra_df, flytting_til_df)

    output_list = []
    if type_of_ds == "historisk":
        output_list = output_historic(input_df)
    elif type_of_ds == "status":
        output_list = output_status(input_df)

    if not output_list:
        raise Exception("No data in outputlist")

    common_aws.write_to_intermediate(output_key=output_prefix, output_list=output_list)


def generate_input_df(flytting_fra_df, flytting_til_df):
    flytting_df = pd.merge(flytting_fra_df, flytting_til_df)

    flytting_df = transform.pivot_table(
        flytting_df,
        pivot_column="innvandringskategori",
        value_columns=[
            "utflytting_fra_oslo",
            "utflytting_innenfor_bydelen",
            "utflytting_mellom_bydeler",
            "innflytting_til_oslo",
            "innflytting_innenfor_bydelen",
            "innflytting_mellom_bydeler",
        ],
    )

    flytting_df["delbydel_id"] = np.nan
    flytting_df["delbydel_navn"] = np.nan

    flytting_df = flytting_df.astype({"delbydel_id": object, "delbydel_navn": object})

    input_df = (
        Aggregate("sum")
        .aggregate(flytting_df, extra_groupby_columns=[aldersgruppe_col])
        .astype({"date": int})
    )

    _add_total_columns(input_df)

    return input_df


def _add_total_columns(input_df):
    input_df[("innflytting_mellom_bydeler", "Total")] = input_df[
        [
            ("innflytting_mellom_bydeler", "Innvandrer"),
            ("innflytting_mellom_bydeler", "Norskfødte med innv.foreldre"),
            ("innflytting_mellom_bydeler", "Øvrige"),
        ]
    ].sum(axis=1)
    input_df[("innflytting_til_oslo", "Total")] = input_df[
        [
            ("innflytting_til_oslo", "Innvandrer"),
            ("innflytting_til_oslo", "Norskfødte med innv.foreldre"),
            ("innflytting_til_oslo", "Øvrige"),
        ]
    ].sum(axis=1)
    input_df[("innflytting_innenfor_bydelen", "Total")] = input_df[
        [
            ("innflytting_innenfor_bydelen", "Innvandrer"),
            ("innflytting_innenfor_bydelen", "Norskfødte med innv.foreldre"),
            ("innflytting_innenfor_bydelen", "Øvrige"),
        ]
    ].sum(axis=1)
    input_df[("utflytting_mellom_bydeler", "Total")] = input_df[
        [
            ("utflytting_mellom_bydeler", "Innvandrer"),
            ("utflytting_mellom_bydeler", "Norskfødte med innv.foreldre"),
            ("utflytting_mellom_bydeler", "Øvrige"),
        ]
    ].sum(axis=1)
    input_df[("utflytting_fra_oslo", "Total")] = input_df[
        [
            ("utflytting_fra_oslo", "Innvandrer"),
            ("utflytting_fra_oslo", "Norskfødte med innv.foreldre"),
            ("utflytting_fra_oslo", "Øvrige"),
        ]
    ].sum(axis=1)
    input_df[("utflytting_innenfor_bydelen", "Total")] = input_df[
        [
            ("utflytting_innenfor_bydelen", "Innvandrer"),
            ("utflytting_innenfor_bydelen", "Norskfødte med innv.foreldre"),
            ("utflytting_innenfor_bydelen", "Øvrige"),
        ]
    ].sum(axis=1)


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
        "mellomBydel": {
            "innvandrer": row_data[("innflytting_mellom_bydeler", "Innvandrer")],
            "norskfødt": row_data[
                ("innflytting_mellom_bydeler", "Norskfødte med innv.foreldre")
            ],
            "øvrige": row_data[("innflytting_mellom_bydeler", "Øvrige")],
            "totalt": row_data[("innflytting_mellom_bydeler", "Total")],
        },
        "tilFraOslo": {
            "innvandrer": row_data[("innflytting_til_oslo", "Innvandrer")],
            "norskfødt": row_data[
                ("innflytting_til_oslo", "Norskfødte med innv.foreldre")
            ],
            "øvrige": row_data[("innflytting_til_oslo", "Øvrige")],
            "totalt": row_data[("innflytting_til_oslo", "Total")],
        },
        "innenforBydel": {
            "innvandrer": row_data[("innflytting_innenfor_bydelen", "Innvandrer")],
            "norskfødt": row_data[
                ("innflytting_innenfor_bydelen", "Norskfødte med innv.foreldre")
            ],
            "øvrige": row_data[("innflytting_innenfor_bydelen", "Øvrige")],
            "totalt": row_data[("innflytting_innenfor_bydelen", "Total")],
        },
    }


def _emigration_object(row_data):
    obj = {
        "alder": row_data[aldersgruppe_col],
        "mellomBydel": {
            "innvandrer": row_data[("utflytting_mellom_bydeler", "Innvandrer")],
            "norskfødt": row_data[
                ("utflytting_mellom_bydeler", "Norskfødte med innv.foreldre")
            ],
            "øvrige": row_data[("utflytting_mellom_bydeler", "Øvrige")],
            "totalt": row_data[("utflytting_mellom_bydeler", "Total")],
        },
        "tilFraOslo": {
            "innvandrer": row_data[("utflytting_fra_oslo", "Innvandrer")],
            "norskfødt": row_data[
                ("utflytting_fra_oslo", "Norskfødte med innv.foreldre")
            ],
            "øvrige": row_data[("utflytting_fra_oslo", "Øvrige")],
            "totalt": row_data[("utflytting_fra_oslo", "Total")],
        },
        "innenforBydel": {
            "innvandrer": row_data[("utflytting_innenfor_bydelen", "Innvandrer")],
            "norskfødt": row_data[
                ("utflytting_innenfor_bydelen", "Norskfødte med innv.foreldre")
            ],
            "øvrige": row_data[("utflytting_innenfor_bydelen", "Øvrige")],
            "totalt": row_data[("utflytting_innenfor_bydelen", "Total")],
        },
    }
    return obj


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
        [value_list] = _values(df)
        return value_list
