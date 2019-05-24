import os

import pandas as pd

import common.aws as common_aws
import common.transform as transform
import common.aggregate_dfs as aggregator
import common.util as util

os.environ["METADATA_API_URL"] = ""

s3_bucket = "ok-origo-dataplatform-dev"

historic_dataset_id = "Landbakgrunn-innvandringsgrupp-87zEq"
historic_version_id = "1-JNRhrxaH"
historic_edition_id = "EDITION-Zkwpy"
status_dataset_id = "Landbakgrunn-innvandringsgrupp-cLcKm"
status_version_id = "1-jwbJpksw"
status_edition_id = "EDITION-FbQet"

pd.set_option("display.max_rows", 1000)


def handle(event, context):
    """ Assuming we recieve a complete s3 key"""
    landbakgrunn_raw = common_aws.read_from_s3(
        s3_key=event["input"]["landbakgrunn-storste-innvandringsgrupper"],
        date_column="aar"
    )
    befolkning_raw = common_aws.read_from_s3(
        s3_key=event["input"]["befolkning-delbydel.kjonn-alder"],
        date_column="aar")

    data_points = ["Innvandrer", "Norskfødt med innvandrerforeldre", "total"]
    input_df = generate_input_df(landbakgrunn_raw, befolkning_raw, data_points)

    output_list = []
    if event['config']['type'] == "status":
        output_list = output_list_status(input_df, data_points, top_n=10)
    elif event['config']['type'] == "historic":
        output_list = output_list_historic(input_df, data_points, top_n=10)

    if output_list:
        _write_to_processed(output_list, event['output'])



def generate_input_df(landbakgrunn_raw, befolkning_raw, data_points):
    befolkning_df = generate_population_df(befolkning_raw)
    # Ignoring Marka and Sentrum
    ignore_districts = ["16", "17"]
    befolkning_district_df = generate_district_population_df(
        befolkning_df, ignore_districts=ignore_districts
    )

    landbakgrunn_df = process_country_df(landbakgrunn_raw)

    input_df = pd.merge(
        landbakgrunn_df, befolkning_district_df, how="inner", on=["district", "date"]
    )
    input_df = aggregator.add_ratios(input_df, data_points, ratio_of=["population"])

    return input_df

def output_list_historic(input_df, data_points, top_n):
    output_list = generate_output_list(
        input_df, data_points, top_n=top_n, template_fun=generate_geo_obj_historic
    )
    return output_list

def output_list_status(input_df, data_points, top_n):
    input_df_status = input_df[input_df["date"] == input_df["date"].max()]
    output_list = generate_output_list(
        input_df_status, data_points, top_n=top_n, template_fun=generate_geo_obj_status
    )
    return output_list

def generate_output_list(input_df, data_points, top_n, template_fun):
    top_n_district = get_top_n_district(input_df, top_n)

    output_list = []
    for district in input_df["district"].unique():
        district_obj = {"district": district, "data": []}
        district_df = input_df[input_df["district"] == district]
        for geography in top_n_district[district]:
            geo_df = district_df[district_df["Landbakgrunn"] == geography]
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
    data_points = ["Innvandrer", "Norskfødt med innvandrerforeldre"]
    df["total"] = df[data_points].sum(axis=1)
    data_points.append("total")
    oslo_total_df = df.groupby(["date", "Landbakgrunn"]).sum().reset_index()
    df["district"] = df["Bydel"].apply(util.get_district_id)
    oslo_total_df["district"] = "00"
    country_df = pd.concat((df, oslo_total_df), sort=False, ignore_index=True)
    return country_df[
        [
            "district",
            "date",
            "Landbakgrunn",
            "Innvandrer",
            "Norskfødt med innvandrerforeldre",
            "total",
        ]
    ]


def generate_district_population_df(population_df, ignore_districts=[]):
    population_df = transform.add_district_id(population_df)
    population_df = population_df[~population_df["district"].isin(ignore_districts)]
    population_district_df = (
        population_df.groupby(["district", "date"]).sum().reset_index()
    )
    oslo_total_df = population_district_df.groupby("date").sum().reset_index()
    oslo_total_df["district"] = "00"
    population_district_df = pd.concat(
        (population_district_df, oslo_total_df), sort=False, ignore_index=True
    )
    return population_district_df


def generate_population_df(df):
    df = df[["delbydelid", "Alder", "Antall personer", "Kjønn", "date"]]
    df = df[df["delbydelid"].notnull()]
    df["population"] = df["Antall personer"]
    df = df[["delbydelid", "date", "population"]]
    df = df.groupby(["delbydelid", "date"]).sum().reset_index()
    return df


def get_top_n_district(df, n):
    top_n = {}
    for district in df["district"].unique():
        district_df = df[df["district"] == district]
        district_df = district_df[district_df["date"] == district_df["date"].max()]
        district_df = district_df.nlargest(n, "total")
        top_n[district] = district_df["Landbakgrunn"].tolist()
    return top_n


def _aggregations(data_points):
    return [
        {"data_points": data_point, "agg_func": "sum"} for data_point in data_points
    ]


def _write_to_processed(output_list, output_key):
    series = [
        {"heading": "Innvandrer", "subheading": ""},
        {"heading": "Norskfødt med innvandrerforeldre", "subheading": ""},
        {"heading": "Totalt", "subheading": ""},
    ]
    heading = "10 største innvandringsgrupper"
    common_aws.write_to_intermediate(output_key, output_list, heading, series)


if __name__ == "__main__":
    handle(
        {
            "input": {
                "landbakgrunn-storste-innvandringsgrupper": "s3/key/to/file.csv",
                "befolkning-delbydel.kjonn-alder": "s3/key/to/file.csv"
            },
            "output": "s3/key/or/prefix",
            "config": {"type": "status"}
        },
        None
    )
    handle(
        {
            "input": {
                "landbakgrunn-storste-innvandringsgrupper": "s3/key/to/file.csv",
                "befolkning-delbydel.kjonn-alder": "raw/yellow/befolkning-etter-kjonn-og-alder/version=1/edition=20190523T211529/Befolkningen_etter_bydel_delbydel_kjonn_og_1-aars_aldersgrupper(1.1.2008-1.1.2019-v01).csv"
            },
            "output": "s3/key/or/prefix",
            "config": {"type": "historic"}
        },
        None
    )
