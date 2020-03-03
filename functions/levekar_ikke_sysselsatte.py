import pandas as pd
from aws_xray_sdk.core import patch_all, xray_recorder
from dataplatform.awslambda.logging import logging_wrapper

import common.aws as common_aws
import common.transform as transform
from common.aggregateV2 import Aggregate
from common.population_utils import generate_population_df
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateB
from common.util import get_min_max_values_and_ratios
from common.event import event_handler

patch_all()

pd.set_option("display.max_rows", 1000)

graph_metadata = Metadata(
    heading="Antall ikke-sysselsatte personer mellom 30-59 år",
    series=[{"heading": "Ikke sysselsatte", "subheading": ""}],
)


@logging_wrapper("levekar_ikke_sysselsatte")
@xray_recorder.capture("event_handler")
@event_handler(
    sysselsatte_raw="sysselsatte", befolkning_raw="befolkning-etter-kjonn-og-alder"
)
def start(sysselsatte_raw, befolkning_raw, output_prefix, type_of_ds):
    data_point = "antall_ikke_sysselsatte"

    input_df = generate_input_df(sysselsatte_raw, befolkning_raw, data_point)

    output_list = []
    if type_of_ds == "historisk":
        output_list = output_historic(input_df, [data_point])

    elif type_of_ds == "status":
        graph_metadata.add_scale(get_min_max_values_and_ratios(input_df, data_point))
        output_list = output_status(input_df, [data_point])

    if output_list:
        common_aws.write_to_intermediate(
            output_key=output_prefix, output_list=output_list
        )
    else:
        raise Exception("No data in outputlist")


def generate_input_df(sysselsatte_raw, befolkning_raw, data_point):

    population_col = "population"

    # Numbers for "sysselsatte" is only for age 30 to 59
    befolkning_df = generate_population_df(befolkning_raw, min_age=30, max_age=59)

    # Value for date in "sysselsatte" was measured in 4th. quarter of 2017, while date for "befolkning" was measured 1.1.2018.
    befolkning_df["date"] = befolkning_df["date"] - 1

    sub_districts = befolkning_df["delbydel_id"].unique()

    sysselsatte_df = sysselsatte_raw
    sysselsatte_df = sysselsatte_df[sysselsatte_df["delbydel_id"].isin(sub_districts)]

    sysselsatte_befolkning_df = pd.merge(
        sysselsatte_df,
        befolkning_df[["date", "delbydel_id", "population"]],
        how="inner",
        on=["date", "delbydel_id"],
    )
    # Ignoring "Marka", "Sentrum" and "Uten registrert adresse"
    ignore_districts = ["16", "17", "99"]
    sysselsatte_befolkning_df = sysselsatte_befolkning_df[
        ~sysselsatte_befolkning_df["bydel_id"].isin(ignore_districts)
    ]

    sysselsatte_befolkning_df[data_point] = (
        sysselsatte_befolkning_df[population_col]
        - sysselsatte_befolkning_df["antall_sysselsatte"]
    )

    agg = Aggregate({population_col: "sum", data_point: "sum"})
    aggregated_df = agg.aggregate(sysselsatte_befolkning_df)

    input_df = agg.add_ratios(
        aggregated_df, data_points=[data_point], ratio_of=[population_col]
    )  # aggregator.add_ratios(aggregated_df, [data_point], ratio_of=[total])
    return input_df


def output_historic(input_df, data_points):
    [input_df] = transform.historic(input_df)
    output = Output(
        values=data_points, df=input_df, metadata=graph_metadata, template=TemplateB()
    ).generate_output()

    return output


def output_status(input_df, data_points):
    [input_df] = transform.status(input_df)

    output = Output(
        values=data_points, df=input_df, metadata=graph_metadata, template=TemplateA()
    ).generate_output()
    return output
