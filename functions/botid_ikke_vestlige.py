import pandas as pd

import common.transform
import common.util as util
import common.transform_output
from common.aws import read_from_s3, write_to_intermediate
from common.templates import TemplateA, TemplateB
from common.aggregateV2 import Aggregate
from common.output import Metadata, Output
from common.population_utils import generate_population_df
from common.util import get_min_max_values_and_ratios

pd.set_option("display.max_rows", 1000)

graph_metadata = Metadata(
    heading="Ikke-vestlige innvandrere korttid",
    series=[
        {
            "heading": "Innvandrere fra Asia, Afrika, Latin-Amerika og Øst-Europa utenfor EU med botid kortere enn fem år",
            "subheading": "",
        }
    ],
)


def handler(event, context):
    """ Assuming we recieve a complete s3 key"""
    s3_key_botid = event["input"]["botid-ikke-vestlige"]
    s3_key_befolkning = event["input"]["befolkning-etter-kjonn-og-alder"]
    output_key = event["output"]
    type_of_ds = event["config"]["type"]
    start(s3_key_botid, s3_key_befolkning, output_key, type_of_ds)
    return "OK"


def start(s3_key_botid, s3_key_befolkning, output_key, type_of_ds):
    botid_ikke_vestlige_raw = read_from_s3(s3_key=s3_key_botid, date_column="aar")

    befolkning_raw = read_from_s3(s3_key=s3_key_befolkning, date_column="aar")

    data_point = "ikke_vestlig_kort"

    df = generate_ikke_vestlig_innvandrer_kort_botid_df(
        botid_ikke_vestlige_raw, befolkning_raw, data_point=data_point
    )

    if type_of_ds == "historisk":
        historic = common.transform.historic(df)
        create_ds(output_key, TemplateB(), [data_point], graph_metadata, *historic)
    elif type_of_ds == "status":
        graph_metadata.add_scale(get_min_max_values_and_ratios(df, data_point))
        status = common.transform.status(df)
        create_ds(output_key, TemplateA(), [data_point], graph_metadata, *status)


def generate_ikke_vestlig_innvandrer_kort_botid_df(
    botid_ikke_vestlige_raw, befolkning_raw, data_point
):

    kort_botid = "Innvandrer, kort botid (<=5 år)"
    ikke_vestlig = "asia_afrika_latin_amerika_og_ost_europa_utenfor_eu"

    df = botid_ikke_vestlige_raw.drop(columns=["norge"])
    df = pivot_table(df, "botid", ikke_vestlig)
    df[data_point] = df[kort_botid]
    df = Aggregate({data_point: "sum"}).aggregate(df)

    population_df = generate_population_df(befolkning_raw)
    population_district_df = Aggregate({"population": "sum"}).aggregate(
        df=population_df
    )

    df = pd.merge(
        df,
        population_district_df[["date", "bydel_id", "delbydel_id", "population"]],
        how="inner",
        on=["bydel_id", "date", "delbydel_id"],
    )

    df = Aggregate({}).add_ratios(
        df=df, data_points=[data_point], ratio_of=["population"]
    )

    return df


def pivot_table(df, pivot_column, value_column):
    key_columns = list(
        filter(lambda x: x not in [pivot_column, value_column], list(df))
    )
    df_pivot = pd.concat(
        (df[key_columns], df.pivot(columns=pivot_column, values=value_column)), axis=1
    )
    return df_pivot.groupby(key_columns).sum().reset_index()


def create_ds(output_key, template, values, metadata, df):
    jsonl = Output(
        df=df, template=template, metadata=metadata, values=values
    ).generate_output()
    write_to_intermediate(output_key=output_key, output_list=jsonl)


if __name__ == "__main__":
    handler(
        {
            "input": {
                "botid-ikke-vestlige": util.get_latest_edition_of(
                    "botid-ikke-vestlige"
                ),
                "befolkning-etter-kjonn-og-alder": util.get_latest_edition_of(
                    "befolkning-etter-kjonn-og-alder", confidentiality="yellow"
                ),
            },
            "output": "intermediate/green/levekar-innvandrere-ikke-vestlige-kort-status/version=1/edition=20190525T183610/",
            "config": {"type": "status"},
        },
        {},
    )
