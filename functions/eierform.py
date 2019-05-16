import os

import pandas as pd
import numpy as np

import common.aws as common_aws
import common.aggregate_dfs as aggregate
import common.transform as transform
from common.transform_output import generate_output_list

os.environ["METADATA_API_URL"] = ""

s3_bucket = "ok-origo-dataplatform-dev"


def handle(event, context):
    s3_key_eierform = event["keys"]["Eierform-av6YC"]
    s3_key_household = event["keys"]["Husholdninger_med_barn-XdfNB"]
    bucket = event["bucket"]
    start(bucket, s3_key_eierform, s3_key_household)
    return "OK"


def start(bucket, key_eierform, key_household):
    #eierform_raw = common_aws.read_from_s3(s3_key=key, date_column="År")

    #eierform_raw = common_aws.read_from_s3(s3_key=key_eierform, date_column="År")
    #household_raw = common_aws.read_from_s3(s3_key=key_household, date_column="År")

    eierform_raw = pd.read_csv("../Eieform(2015-2017-v01).csv", sep=";", dtype={"delbydelid": object}).rename(columns={"År": "date", "Leier alle": "rent", " Borettlslag-andel alle": "share", "Selveier alle": "owner"})
    household_raw = pd.read_csv("../Husholdninger_med_barn(1.1.2008-1.1.2018-v01).csv", sep=";", dtype={"delbydelid": object}).rename(columns={"År": "date"})

    eierform_raw = eierform_raw.drop(['Borettlslag-andel uten studenter', 'Selveier uten studenter', 'Leier uten studenter'], axis=1)

    household_raw = household_raw[household_raw['date'] >= 2015]
    
    print(household_raw)

    data_points_eierform = ["Leier alle", " Borettlslag-andel alle", "Selveier alle"]
    data_points_households = ["all_households"]

    with_district_eierform = transform.add_district_id(eierform_raw.copy())
    with_district_household = transform.add_district_id(household_raw.copy())

    with_data_points_households = with_household_data_points(with_district_household)
    with_data_points_eierform = with_district_eierform.groupby(["delbydelid", "date", "district"]).agg(lambda x: x).reset_index()

    print(with_data_points_households)

    #input_df_eierform = aggregate_to_input_format(with_district_eierform, data_points_eierform)
    input_df_household = aggregate_to_input_format(with_data_points_households, data_points_households)

    new_df = aggregate.merge_dfs(with_data_points_eierform, input_df_household)

    print(new_df)

    input_new_df =aggregate_to_input_format(new_df, data_points_households)

    print(input_new_df)

    #eierform_historic = generate_output_list(*transform.historic(with_data_points), template="c", data_points=data_points)

    #eierform_status = generate_output_list(*transform.status(with_data_points), template="a", data_points=data_points)

    #print(eierform_historic)
    #print(eierform_status)


def _aggregations(data_points):
    return [
        {"data_points": data_point, "agg_func": "sum"} for data_point in data_points
    ]


def aggregate_to_input_format(df, data_points):
    aggregations = _aggregations(data_points)
    input_df = aggregate.aggregate_from_subdistricts(df, aggregations)
    return input_df


def with_household_data_points(household_raw):
    household_raw["household_data_point"] = household_raw["Husholdningstype"].apply(
        household_data_point
    )

    with_data_points = pd.concat(
        (
            household_raw[["date", "district", "delbydelid"]],
            household_raw.pivot(
                columns="household_data_point", values="Antall husholdninger"
            ),
        ),
        axis=1,
    )

    return (
        with_data_points.groupby(["delbydelid", "date", "district"]).sum().reset_index()
    )


def household_data_point(household_type):
    all_households = [
        "Mor/far med små barn",
        "Mor/far med store barn",
        "Par med små barn",
        "Par med store barn",
        "Enfamiliehusholdninger med voksne barn",
        "Flerfamiliehusholdninger med små barn",
        "Flerfamiliehusholdninger med store barn",
        "Flerfamiliehusholdninger uten barn 0 - 17 år",
        "Par uten hjemmeboende barn",
        "Flerfamiliehusholdninger uten barn 0-17 år",
        "Aleneboende"
    ]

    if household_type in all_households:
        return "all_households"
    else:
        raise Exception(f"No data_point for Hushodningstype={household_type}")



if __name__ == "__main__":
    handle(
        {
            "bucket": "ok-origo-dataplatform-dev",
            "keys": {
                "Eierform-av6YC": "raw/green/Eieform-av6YC/version=1-VuiEHRh8/edition=EDITION-mL7Lf/Eieform(2015-2017-v01).csv",
                "Husholdninger_med_barn-XdfNB": "raw/green/Husholdninger_med_barn-XdfNB/version=1-oTr62ZHJ/edition=EDITION-ivaYi/Husholdninger_med_barn(1.1.2008-1.1.2018-v01).csv"
            }
        },
        {}
    )