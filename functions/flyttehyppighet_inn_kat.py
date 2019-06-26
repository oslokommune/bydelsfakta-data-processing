import pandas as pd
import numpy as np

import common.transform as transform
import common.aws as common_aws
from common.aggregateV2 import ColumnNames
from common.util import get_latest_edition_of
from common.output import Output, Metadata
from common.templates import Template

graph_metadata = Metadata(
    heading="Flytting med innvandringskategorier",
    series=[{"heading": "Flytting med innvandringskategorier", "subheading": ""}],
)

flytting_fra_etter_inn_kat_id = "flytting-fra-etter-inn-kat"
flytting_til_etter_inn_kat_id = "flytting-til-etter-inn-kat"

aldersgruppe_col = "aldersgruppe_5_aar"
key_cols = ["date", "bydel_id", "bydel_navn", aldersgruppe_col]


def handle(event, context):
    s3_key_flytting_fra_etter_inn_kat_raw = event["input"][
        flytting_fra_etter_inn_kat_id
    ]
    s3_key_flytting_til_etter_inn_kat_raw = event["input"][
        flytting_til_etter_inn_kat_id
    ]

    output_key = event["output"]
    type_of_ds = event["config"]["type"]

    input_df = generate_input_df(
        s3_key_flytting_fra_etter_inn_kat_raw, s3_key_flytting_til_etter_inn_kat_raw
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
    flytting_fra_df = common_aws.read_from_s3(
        s3_key=s3_key_flytting_fra_raw, date_column="aar"
    )
    flytting_til_df = common_aws.read_from_s3(
        s3_key=s3_key_flytting_til_raw, date_column="aar"
    )

    flytting_fra_df["utflytting"] = flytting_fra_df[
        ["utflytting_fra_oslo", "utflytting_mellom_bydeler"]
    ].sum(axis=1)
    flytting_til_df["innflytting"] = flytting_til_df[
        ["innflytting_til_oslo", "innflytting_mellom_bydeler"]
    ].sum(axis=1)

    flytting_df = pd.merge(flytting_fra_df, flytting_til_df)[
        [*key_cols, "innvandringskategori", "utflytting", "innflytting"]
    ]

    flytting_df = transform.pivot_table(
        flytting_df,
        pivot_column="innvandringskategori",
        value_columns=["utflytting", "innflytting"],
    )

    flytting_df["delbydel_id"] = np.nan
    flytting_df["delbydel_navn"] = np.nan

    flytting_df[("utflytting", "total")] = flytting_df[
        [
            ("utflytting", "Innvandrer"),
            ("utflytting", "Norskfødte med innv.foreldre"),
            ("utflytting", "Øvrige"),
        ]
    ].sum(axis=1)

    flytting_df[("innflytting", "total")] = flytting_df[
        [
            ("innflytting", "Innvandrer"),
            ("innflytting", "Norskfødte med innv.foreldre"),
            ("innflytting", "Øvrige"),
        ]
    ].sum(axis=1)

    return flytting_df


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
        "totalt": row_data[("innflytting", "total")],
        "innvandrer": row_data[("innflytting", "Innvandrer")],
        "norskfødt": row_data[("innflytting", "Norskfødte med innv.foreldre")],
        "øvrige": row_data[("innflytting", "Øvrige")],
    }


def _emigration_object(row_data):
    return {
        "alder": row_data[aldersgruppe_col],
        "totalt": row_data[("utflytting", "total")],
        "innvandrer": row_data[("utflytting", "Innvandrer")],
        "norskfødt": row_data[("utflytting", "Norskfødte med innv.foreldre")],
        "øvrige": row_data[("utflytting", "Øvrige")],
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


if __name__ == "__main__":
    flytting_fra_etter_inn_kat_s3_key = get_latest_edition_of(
        flytting_fra_etter_inn_kat_id
    )
    flytting_til_etter_inn_kat_s3_key = get_latest_edition_of(
        flytting_til_etter_inn_kat_id
    )
    handle(
        {
            "input": {
                flytting_fra_etter_inn_kat_id: flytting_fra_etter_inn_kat_s3_key,
                flytting_til_etter_inn_kat_id: flytting_til_etter_inn_kat_s3_key,
            },
            "output": "s3/key/or/prefix",
            "config": {"type": "historisk"},
        },
        None,
    )
