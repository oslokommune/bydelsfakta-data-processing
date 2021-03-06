import pandas as pd
from aws_xray_sdk.core import patch_all, xray_recorder
from dataplatform.awslambda.logging import logging_wrapper

import common.transform
import common.transform_output
from common.aws import write_to_intermediate
from common.templates import TemplateA, TemplateB
from common.aggregateV2 import Aggregate
from common.output import Metadata, Output
from common.population_utils import generate_population_df
from common.util import get_min_max_values_and_ratios
from common.event import event_handler

patch_all()

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


@logging_wrapper("botid_ikke_vestlige")
@xray_recorder.capture("event_handler")
@event_handler(
    botid_ikke_vestlige_raw="botid-ikke-vestlige",
    befolkning_raw="befolkning-etter-kjonn-og-alder",
)
def start(botid_ikke_vestlige_raw, befolkning_raw, output_prefix, type_of_ds):
    data_point = "ikke_vestlig_kort"

    df = generate_ikke_vestlig_innvandrer_kort_botid_df(
        botid_ikke_vestlige_raw, befolkning_raw, data_point=data_point
    )

    if type_of_ds == "historisk":
        historic = common.transform.historic(df)
        create_ds(output_prefix, TemplateB(), [data_point], graph_metadata, *historic)
    elif type_of_ds == "status":
        graph_metadata.add_scale(get_min_max_values_and_ratios(df, data_point))
        status = common.transform.status(df)
        create_ds(output_prefix, TemplateA(), [data_point], graph_metadata, *status)


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
