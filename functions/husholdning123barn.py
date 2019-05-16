import os
import sys  # TEMP FOR DEBUG

import pandas as pd
import numpy as np

import common.aws as common_aws
import common.aggregate_dfs as aggregate
import common.transform as transform
from common.transform_output import generate_output_list

os.environ["METADATA_API_URL"] = ""

s3_bucket = "ok-origo-dataplatform-dev"

datasets = [{"dataset_title": "Husholdninger med 1 barn",
             "dataset_id": "husholdning_1-barn_status",
             "version_id": "",
             "edition_id": "",
             "data_point": "Barn 1"},
            {"dataset_title": "Husholdninger med 2 barn",
             "dataset_id": "husholdning_2-barn_status",
             "version_id": "",
             "edition_id": "",
             "data_point": "Barn 2"},
            {"dataset_title": "Husholdninger med 3+ barn",
             "dataset_id": "husholdning_3-barn_status",
             "version_id": "",
             "edition_id": "",
             "data_point": "Barn 3+"}]

#historic_dataset_id = "Husholdning-totalt-historisk-NZrxf"
#historic_version_id = "1-Xh69qF9c"
#historic_edition_id = "EDITION-gvinX"
#status_dataset_id = "Husholdning-totalt-status-FzFf5"
#status_version_id = "1-wgHdAJWY"
#status_edition_id = "EDITION-nibxq"
#matrix_dataset_id = "Husholdning-totalt-matrise-e9w4m"
#matrix_version_id = "1-8fZLJ6dC"
#matrix_edition_id = "EDITION-756ZB"

def handle(event, context):

    """ Assuming we recieve a complete s3 key"""
    s3_key = event["keys"]["Husholdninger_med_barn-XdfNB"]
    bucket = event["bucket"]
    start(bucket, s3_key)

    return "OK"


def start(bucket, key):

    household_raw = common_aws.read_from_s3(s3_key=key)

    household_df = household_raw.copy()
    household_df["Antall husholdninger"] = household_df["Antall husholdninger"].str.replace(" ", "").astype(int)

    # TEMP! Rename "Delbydel" --> "district" <== This needs to be done for husholdning.py as well.
    household_df = household_df.rename(columns={"Delbydel": "district"})
    household_df = transform.add_district_id(household_df.copy())
    household_df = household_df[
        [
            "delbydelid",
            "district",
            "date",
            "Barn i husholdningen",
            "Antall husholdninger"
        ]
    ]

    m = {
        "Ingen barn i HH": "Barn 0",
        "1 barn i HH": "Barn 1",
        "2 barn i HH": "Barn 2",
        "3 barn i HH": "Barn 3+",
        "4 barn eller mer": "Barn 3+"
    }
    household_df["Barnekategori"] = (
        household_df["Barn i husholdningen"].map(m).fillna(np.nan)
    )
    if household_df["Barnekategori"].isnull().any():
        raise ValueError("Unmapped category found in the column Husholdningstype. See dict 'm'.")
    household_df = household_df.drop("Barn i husholdningen", axis=1)

    # Reshape from group data by rows to group data by col
    household_df = household_df.groupby(
        ["delbydelid", "district", "date", "Barnekategori"], as_index=False
    ).agg("sum")
    household_df = household_df.pivot_table(
        index=["delbydelid", "district", "date"],
        columns="Barnekategori",
        values="Antall husholdninger",
        fill_value=0,
    ).reset_index(drop=False)

    df_status = transform.status(household_df)[0]
    df_agg = aggregate_to_input_format(df_status, ["Barn 0", "Barn 1", "Barn 2", "Barn 3+"])

    for dataset in datasets:

        print(dataset["dataset_id"])

        #print(generate_output_list(df_agg, template="a", data_points=[dataset["data_point"]]))

        dataset["data"] = generate_output_list(df_agg, template="a", data_points=[dataset["data_point"]])

        # TO BE REMOVED!
        debug = False
        if debug:
            from pprint import pprint
            with open(r"C:\CURRENT FILES\dump.txt", "wt", encoding="utf-8") as f:
                pprint(dataset["data"], stream=f)
                print("\n", file=f)
                sys.exit(1)


        print("2019-May-16 13:00: Need to fill in {data_sets} - awaiting pipeline readiness!")
        sys.exit(1)
        ####
        _write_to_intermediate(dataset["dataset_id"],
                               dataset["version_id"],
                               dataset["edition_id"],
                               dataset["data"],
                               series_heading=dataset["dataset_title"])

def _aggregations(data_points):

    """Return a list of aggreation dicts (given that 'sum' should be used). To be used when later doing the aggreations."""

    return [
        {"data_points": data_point, "agg_func": "sum"} for data_point in data_points
    ]


def aggregate_to_input_format(df, data_points):
    aggregations = _aggregations(data_points)
    input_df = aggregate.aggregate_from_subdistricts(df, aggregations)
    input_df = aggregate.add_ratios(input_df, data_points, data_points)
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
    with_children = [
        "Mor/far med små barn",
        "Mor/far med store barn",
        "Par med små barn",
        "Par med store barn",
        "Enfamiliehusholdninger med voksne barn",
        "Flerfamiliehusholdninger med små barn",
        "Flerfamiliehusholdninger med store barn",
    ]
    no_children = [
        "Flerfamiliehusholdninger uten barn 0 - 17 år",
        "Par uten hjemmeboende barn",
        "Flerfamiliehusholdninger uten barn 0-17 år",
    ]
    single_adult = ["Aleneboende"]

    if household_type in with_children:
        return "with_children"
    elif household_type in no_children:
        return "no_children"
    elif household_type in single_adult:
        return "single_adult"
    else:
        raise Exception(f"No data_point for Hushodningstype={household_type}")


def _output_key(dataset_id, version_id, edition_id):
    return f"processed/green/{dataset_id}/version={version_id}/edition={edition_id}/"


def _write_to_intermediate(dataset_id, version_id, edition_id, output_list, series_heading):

    series = [
        {"heading": series_heading, "subheading": ""},
    ]
    heading = "Husholdninger"
    output_key = _output_key(dataset_id, version_id, edition_id)
    common_aws.write_to_intermediate(output_key, output_list, heading, series)


if __name__ == "__main__":

    handle(
        {
            "bucket": "ok-origo-dataplatform-dev",
            "keys": {
                "Husholdninger_med_barn-XdfNB": "raw/green/Husholdninger_med_barn-XdfNB/version=1-oTr62ZHJ/1557487278/Husholdninger_med_barn(1.1.2008-1.1.2018-v02).csv"
            },
        },
        {},
    )
