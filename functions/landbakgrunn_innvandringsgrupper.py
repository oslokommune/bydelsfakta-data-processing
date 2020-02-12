import pandas as pd
from aws_xray_sdk.core import patch_all, xray_recorder
from dataplatform.awslambda.logging import logging_wrapper

import common.aws as common_aws
import common.aggregate_dfs as aggregator
import common.population_utils as population_utils
from common.aggregateV2 import Aggregate
from common.output import Metadata
from common.util import get_latest_edition_of
from common.event import event_handler

patch_all()
pd.set_option("display.max_rows", 1000)


@logging_wrapper("landbakgrunn_innvandringsgrupper__old")
@xray_recorder.capture("handler_old")
def handler_old(event, context):
    s3_key_landbakgrunn = event["input"]["landbakgrunn-storste-innvandringsgrupper"]
    s3_key_befolkning = event["input"]["befolkning-etter-kjonn-og-alder"]
    output_key = event["output"]
    type_of_ds = event["config"]["type"]
    landbakgrunn_raw = common_aws.read_from_s3(
        s3_key=s3_key_landbakgrunn, date_column="aar"
    )
    befolkning_raw = common_aws.read_from_s3(
        s3_key=s3_key_befolkning, date_column="aar"
    )
    start(landbakgrunn_raw, befolkning_raw, output_key, type_of_ds)
    return f"Created {output_key}"


@logging_wrapper("landbakgrunn_innvandringsgrupper")
@xray_recorder.capture("event_handler")
@event_handler(
    landbakgrunn_raw="landbakgrunn-storste-innvandringsgrupper",
    befolkning_raw="befolkning-etter-kjonn-og-alder",
)
def _start(*args, **kwargs):
    start(*args, **kwargs)


def start(landbakgrunn_raw, befolkning_raw, output_prefix, type_of_ds):
    data_points_status = ["innvandrer", "norskfodt_med_innvandrerforeldre"]
    data_points = ["total", "innvandrer", "norskfodt_med_innvandrerforeldre"]

    input_df = generate_input_df(landbakgrunn_raw, befolkning_raw, data_points)

    output_list = []

    if type_of_ds == "status":
        output_list = output_list_status(input_df, data_points_status, top_n=10)
    elif type_of_ds == "historisk":
        output_list = output_list_historic(input_df, data_points, top_n=10)

    if output_list:
        common_aws.write_to_intermediate(
            output_key=output_prefix, output_list=output_list
        )
    else:
        raise Exception("No data in outputlist")


def generate_input_df(landbakgrunn_raw, befolkning_raw, data_points):
    befolkning_df = population_utils.generate_population_df(befolkning_raw)
    # Ignoring Marka and Sentrum
    ignore_districts = ["16", "17"]
    befolkning_district_df = population_district_only(
        befolkning_df, ignore_districts=ignore_districts
    )

    landbakgrunn_df = process_country_df(landbakgrunn_raw)

    input_df = pd.merge(
        landbakgrunn_df,
        befolkning_district_df,
        how="inner",
        on=["bydel_id", "date", "bydel_navn"],
    )
    input_df = aggregator.add_ratios(input_df, data_points, ratio_of=["population"])

    return input_df


def output_list_historic(input_df, data_points, top_n):
    graph_metadata = Metadata(
        series=[
            {"heading": "Totalt", "subheading": ""},
            {"heading": "Innvandrer", "subheading": ""},
            {"heading": "Norskfødt med innvandrerforeldre", "subheading": ""},
        ],
        heading="10 største innvandringsgrupper",
    )

    output_list = generate_output_list(
        input_df,
        data_points,
        top_n=top_n,
        template_fun=generate_geo_obj_historic,
        graph_metadata=graph_metadata,
    )
    return output_list


def output_list_status(input_df, data_points, top_n):
    graph_metadata = Metadata(
        series=[
            {"heading": "Innvandrer", "subheading": ""},
            {"heading": "Norskfødt med innvandrerforeldre", "subheading": ""},
        ],
        heading="10 største innvandringsgrupper",
    )

    input_df_status = input_df[input_df["date"] == input_df["date"].max()]
    output_list = generate_output_list(
        input_df_status,
        data_points,
        top_n=top_n,
        template_fun=generate_geo_obj_status,
        graph_metadata=graph_metadata,
    )
    return output_list


def generate_output_list(input_df, data_points, top_n, template_fun, graph_metadata):
    graph_metadata_as_dict = {
        "series": graph_metadata.series,
        "heading": graph_metadata.heading,
    }

    top_n_countries = get_top_n_countries(input_df, top_n)

    district_list = [
        (x.bydel_id, x.bydel_navn)
        for x in set(
            input_df.loc[:, ["bydel_id", "bydel_navn"]].itertuples(index=False)
        )
    ]
    output_list = []
    for district_id, district_name in district_list:
        district_obj = {
            "district": district_name,
            "id": district_id,
            "data": [],
            "meta": graph_metadata_as_dict,
        }
        district_df = input_df[input_df["bydel_id"] == district_id]
        for geography in top_n_countries[district_id]:
            geo_df = district_df[district_df["landbakgrunn"] == geography]
            geo_obj = template_fun(geo_df, geography, data_points)
            district_obj["data"].append(geo_obj)
        output_list.append(district_obj)

    return output_list


def generate_geo_obj_status(df, geography, data_points):
    series = {}
    for value in df.to_dict("r"):
        for data_point in data_points:
            series[data_point] = {
                "date": value["date"],
                "value": value[data_point],
                "ratio": value[f"{data_point}_ratio"],
            }
    values = [series[data_point] for data_point in data_points if series]
    return {"geography": geography, "values": values}


def generate_geo_obj_historic(df, geography, data_points):
    series = {data_point: [] for data_point in data_points}
    for value in df.to_dict("r"):
        for data_point in data_points:
            series[data_point].append(
                {
                    "date": value["date"],
                    "value": value[data_point],
                    "ratio": value[f"{data_point}_ratio"],
                }
            )
    values = [series[data_point] for data_point in data_points if series]
    return {"geography": geography, "values": values}


def process_country_df(df):
    data_points = ["innvandrer", "norskfodt_med_innvandrerforeldre"]
    df["total"] = df[data_points].sum(axis=1)
    data_points.append("total")
    oslo_total_df = df.groupby(["date", "landbakgrunn"]).sum().reset_index()
    oslo_total_df["bydel_id"] = "00"
    oslo_total_df["bydel_navn"] = "Oslo i alt"
    country_df = pd.concat((df, oslo_total_df), sort=False, ignore_index=True)
    return country_df[
        [
            "bydel_id",
            "bydel_navn",
            "date",
            "landbakgrunn",
            "innvandrer",
            "norskfodt_med_innvandrerforeldre",
            "total",
        ]
    ]


def population_district_only(population_df, ignore_districts=[]):
    population_df = population_df[~population_df["bydel_id"].isin(ignore_districts)]
    agg = {"population": "sum"}
    population_aggregated_df = Aggregate(agg).aggregate(population_df)
    population_district_only_df = population_aggregated_df[
        population_aggregated_df["delbydel_id"].isnull()
    ]
    return population_district_only_df


def get_top_n_countries(df, n):
    top_n = {}
    for district in df["bydel_id"].unique():
        district_df = df[df["bydel_id"] == district]
        district_df = district_df[district_df["date"] == district_df["date"].max()]
        district_df = district_df.nlargest(n, "total")
        top_n[district] = district_df["landbakgrunn"].tolist()
    return top_n


if __name__ == "__main__":
    landbakgrunn_storste_innvandringsgrupper = get_latest_edition_of(
        "landbakgrunn-storste-innvandringsgrupper"
    )
    befolkning_etter_kjonn_og_alder = get_latest_edition_of(
        "befolkning-etter-kjonn-og-alder", confidentiality="yellow"
    )
    handler_old(
        {
            "input": {
                "landbakgrunn-storste-innvandringsgrupper": landbakgrunn_storste_innvandringsgrupper,
                "befolkning-etter-kjonn-og-alder": befolkning_etter_kjonn_og_alder,
            },
            "output": "intermediate/green/landbakgrunn-innvandringsgrupper-status/version=1/edition=20190703T102550/",
            "config": {"type": "status"},
        },
        None,
    )
