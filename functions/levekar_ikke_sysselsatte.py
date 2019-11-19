import pandas as pd

import common.aws as common_aws
import common.transform as transform
from common.aggregateV2 import Aggregate
from common.population_utils import generate_population_df
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateB
from common.util import get_latest_edition_of, get_min_max_values_and_ratios

pd.set_option("display.max_rows", 1000)

graph_metadata = Metadata(
    heading="Antall ikke-sysselsatte personer mellom 30-59 år",
    series=[{"heading": "Ikke sysselsatte", "subheading": ""}],
)


def handle(event, context):
    s3_key_sysselsatte = event["input"]["sysselsatte"]
    s3_key_befolkning = event["input"]["befolkning-etter-kjonn-og-alder"]
    output_key = event["output"]
    type_of_ds = event["config"]["type"]

    data_point = "antall_ikke_sysselsatte"

    sysselsatte_raw = common_aws.read_from_s3(
        s3_key=s3_key_sysselsatte, date_column="aar"
    )
    befolkning_raw = common_aws.read_from_s3(
        s3_key=s3_key_befolkning, date_column="aar"
    )
    input_df = generate_input_df(sysselsatte_raw, befolkning_raw, data_point)

    output_list = []
    if type_of_ds == "historisk":
        output_list = output_historic(input_df, [data_point])

    elif type_of_ds == "status":
        graph_metadata.add_scale(get_min_max_values_and_ratios(input_df, data_point))
        output_list = output_status(input_df, [data_point])

    if output_list:
        common_aws.write_to_intermediate(output_key=output_key, output_list=output_list)
        return f"Created {output_key}"

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


if __name__ == "__main__":
    sysselsatte_s3_key = get_latest_edition_of("sysselsatte")
    befolkning_s3_key = get_latest_edition_of(
        "befolkning-etter-kjonn-og-alder", confidentiality="yellow"
    )
    handle(
        {
            "input": {
                "sysselsatte": sysselsatte_s3_key,
                "befolkning-etter-kjonn-og-alder": befolkning_s3_key,
            },
            "output": "intermediate/green/levekar-ikke-sysselsatte-status/version=1/edition=20191111T144000/",
            "config": {"type": "status"},
        },
        None,
    )
    # handle(
    #     {
    #         "input": {
    #             "sysselsatte": sysselsatte_s3_key,
    #             "befolkning-etter-kjonn-og-alder": befolkning_s3_key,
    #         },
    #         "output": "s3/key/or/prefix",
    #         "config": {"type": "historisk"},
    #     },
    #     None,
    # )
