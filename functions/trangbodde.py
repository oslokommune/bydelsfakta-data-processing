import os

import pandas as pd

import common.aws as common_aws
import common.aggregate_dfs as aggregator
import common.population_utils as population_utils
import numpy as np

os.environ["METADATA_API_URL"] = ""

pd.set_option("display.max_rows", 1000)


def handle(event, context):
    """ Assuming we recieve a complete s3 key"""
    landbakgrunn_raw = common_aws.read_from_s3(
        s3_key=event["input"]["trangbodde"],
        date_column="aar"
    )
    befolkning_raw = common_aws.read_from_s3(
        s3_key=event["input"]["befolkning-etter-kjonn-og-alder"],
        date_column="aar")

    datapoint = 'antall_trangbodde'
    input_df = generate_input_df(landbakgrunn_raw, befolkning_raw, datapoint)

    return input_df


def generate_input_df(trangbodde_raw, population_raw, data_point):

    population_df = population_utils.generate_population_df(population_raw)
    # Ignoring Marka and Sentrum
    ignore_districts = ["16", "17", "99"]
    population_district_df = population_with_district_aggregated(
        population_df, ignore_districts=ignore_districts
    )

    trangbodde_raw['bydel_id'] = trangbodde_raw['bydel_id'].apply(convert_stupid_district_id)

    input_df = pd.merge(
        trangbodde_raw, population_district_df, how="inner", on=["bydel_id", "date", "delbydel_id"]
    ).rename(
        columns={'bydel_navn_x': 'bydel_navn',
                 'delbydel_navn_x': 'delbydel_navn'}
    )
    input_df[f'{data_point}_ratio'] = input_df['andel_som_bor_trangt']/100
    input_df[data_point] = input_df['population']*input_df[f'{data_point}_ratio']

    return input_df[['date', 'bydel_id', 'bydel_navn', 'delbydel_id', 'delbydel_navn', data_point, f'{data_point}_ratio']]


def convert_stupid_district_id(possibly_stupid_id):
    if possibly_stupid_id == 10000:
        return 0
    else:
        return possibly_stupid_id

def population_with_district_aggregated(population_df, ignore_districts=[]):
    population_df = population_df[~population_df["bydel_id"].isin(ignore_districts)]
    population_district_df = (
        population_df.groupby(["bydel_id", "bydel_navn", "date"])['population'].sum().reset_index()
    )
    oslo_total_df = population_district_df.groupby("date").sum().reset_index()
    oslo_total_df["bydel_id"] = 00
    oslo_total_df["bydel_navn"] = "Oslo i alt"
    population_district_df = pd.concat(
        (population_district_df, oslo_total_df), sort=False, ignore_index=True
    )
    population_district_df['delbydel_id'] = np.nan
    population_district_df['delbydel_navn'] = ''
    population_district_df = pd.concat(
        (population_df, population_district_df), sort=False, ignore_index=True
    )
    return population_district_df


if __name__ == "__main__":
    # handle(
    #     {
    #         "input": {
    #             "trangbodde": "raw/green/trangbodde/version=1/edition=20190524T112022/Trangbodde(1.1.2015-1.1.2017-v01).csv",
    #             "befolkning-etter-kjonn-og-alder": "raw/yellow/befolkning-etter-kjonn-og-alder/version=1/edition=20190523T211529/Befolkningen_etter_bydel_delbydel_kjonn_og_1-aars_aldersgrupper(1.1.2008-1.1.2019-v01).csv"
    #         },
    #         "output": "s3/key/or/prefix",
    #         "config": {"type": "status"}
    #     },
    #     None
    # )
    handle(
        {
            "input": {
                "trangbodde": "raw/green/trangbodde/version=1/edition=20190524T112022/Trangbodde(1.1.2015-1.1.2017-v01).csv",
                "befolkning-etter-kjonn-og-alder": "raw/yellow/befolkning-etter-kjonn-og-alder/version=1/edition=20190523T211529/Befolkningen_etter_bydel_delbydel_kjonn_og_1-aars_aldersgrupper(1.1.2008-1.1.2019-v01).csv"
            },
            "output": "s3/key/or/prefix",
            "config": {"type": "historic"}
        },
        None
    )