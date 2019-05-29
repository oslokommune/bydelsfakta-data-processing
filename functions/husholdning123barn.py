import os
import sys  # TEMP FOR DEBUG

import pandas as pd
import numpy as np

import common.aws
import common.aggregate_dfs # as aggregate
import common.transform # as transform
from common.transform_output import generate_output_list
from common.aggregateV2 import ColumnNames, Aggregate

os.environ["METADATA_API_URL"] = ""

s3_bucket = "ok-origo-dataplatform-dev"

def handler(event, context):

    """ Assuming we receive a complete s3 key"""

    s3_key = event["input"]["husholdninger-med-barn"]
    output_key = event["output"]
    type_of_ds = event["config"]["type"]
    start(s3_key, output_key, type_of_ds)

    return "OK"


def start(key, output_key, type_of_ds):

    number_type = "float64"  # "int64" - this SHOULD be used when the input data are integers.

    dtype = {"delbydel_id": object,
             "delbydel_navn": object,
             "bydel_id": object,
             "bydel_navn": object,
             "aleneboende": number_type,
             "enfamiliehusholdninger_med_voksne_barn": number_type,
             "flerfamiliehusholdninger_med_smaa_barn": number_type,
             "flerfamiliehusholdninger_med_store_barn": number_type,
             "flerfamiliehusholdninger_uten_barn_0_til_17_aar": number_type,
             "mor_far_med_smaa_barn": number_type,
             "mor_far_med_store_barn": number_type,
             "par_med_smaa_barn": number_type,
             "par_med_store_barn": number_type,
             "par_uten_hjemmeboende_barn": number_type}

    df = common.aws.read_from_s3(s3_key=key, date_column="aar", dtype=dtype)

    df = _clean_df(df)

    df = _remap_number_of_children(df)

    # Pivot - values for categories in "barnekategori" move to separate columns
    df = df.pivot_table(
        index=["date", "bydel_id", "bydel_navn", "delbydel_id", "delbydel_navn"],
        columns="barnekategori",
        values="antall_husholdninger",
        fill_value=0,
    ).reset_index(drop=False)

    # Keep only data from the last year in the data set
    df = common.transform.status(df)[0].reset_index(drop=True)

    print(df.head())

    #sys.exit(1)

    # hus = datasets_for_testing.husholdinger.content()

    # Aggregate
    aggregation = {"Barn 0": "sum",
                   "Barn 1": "sum",
                   "Barn 2": "sum",
                   "Barn 3+": "sum"}
    agg = Aggregate(aggregation)
    df_agg = agg.aggregate(df)
    import time
    time.sleep(1)
    print(df_agg)

    sys.exit(1)



    df = common.aggregate_dfs.aggregate_from_subdistricts(df, ["Barn 0", "Barn 1", "Barn 2", "Barn 3+"])

    print(df)
    sys.exit(1)


    for dataset in datasets:

        print(dataset["dataset_id"])

        # print(generate_output_list(df_agg, template="a", data_points=[dataset["data_point"]]))

        dataset["data"] = generate_output_list(
            df_agg, template="a", data_points=[dataset["data_point"]]
        )

        # TO BE REMOVED!
        debug = False
        if debug:
            from pprint import pprint

            with open(r"C:\CURRENT FILES\dump.txt", "wt", encoding="utf-8") as f:
                pprint(dataset["data"], stream=f)
                print("\n", file=f)
                sys.exit(1)

        print(
            "2019-May-16 13:00: Need to fill in {data_sets} - awaiting pipeline readiness!"
        )
        sys.exit(1)
        ####
        _write_to_intermediate(
            dataset["dataset_id"],
            dataset["version_id"],
            dataset["edition_id"],
            dataset["data"],
            series_heading=dataset["dataset_title"],
        )


def _clean_df(df):

    """Various operations to clean and secure correct format."""

    df = df.rename(columns={"barn_i_hushodlningen": "barn_i_husholdningen"})  # Temp minor colname cleanup

    mandatory = {'date', 'delbydel_id', 'delbydel_navn', 'bydel_id', 'bydel_navn', 'barn_i_husholdningen'}
    for m in mandatory:
        assert (m in df.columns)

    number_cols = set(df.columns).difference(mandatory)
    for nc in number_cols:
        df[nc] = df[nc].round().astype("int64")  # This line should be removed when the input format is secured.
        assert (df[nc].dtype == "int64")

    df["antall_husholdninger"] = df[number_cols].sum(axis=1)

    df = df[[*mandatory, "antall_husholdninger"]]

    return (df)


def _remap_number_of_children(df):

    """Map to the required format for number of children in the household."""

    m = {
        "Ingen barn i HH": "Barn 0",
        "1 barn i HH": "Barn 1",
        "2 barn i HH": "Barn 2",
        "3 barn i HH": "Barn 3+",
        "4 barn eller mer": "Barn 3+",
    }
    df["barnekategori"] = (
        df["barn_i_husholdningen"].map(m).fillna(np.nan)
    )
    if df["barnekategori"].isnull().any():
        raise ValueError(
            "Unmapped category found in the column 'barn_i_husholdningen'. See dict 'm'."
        )
    df = df.drop("barn_i_husholdningen", axis=1)

    return df


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


def _write_to_intermediate(
    dataset_id, version_id, edition_id, output_list, series_heading
):

    series = [{"heading": series_heading, "subheading": ""}]
    heading = "Husholdninger"
    output_key = _output_key(dataset_id, version_id, edition_id)
    common_aws.write_to_intermediate(output_key, output_list, heading, series)


if __name__ == "__main__":

    # Inntil videre jobb med v1
    # UPDATE input path when v2 is uploaded by Tord. <== KRASJER FORDI ANTALL ER STRENGER MED MELLOMROM
    # UPDATE: Tord har kontaktet Astrid som skal skaffe en V3.
    # husholdninger-med-1-barn
    # husholdninger-med-2-barn
    # husholdninger-med-3-barn
    # OUTPUT-dataset
    # "husholdninger-med-barn": "raw/green/husholdninger-med-barn/version=1/edition=20190523T125413/Husholdninger_med_barn(1.1.2008-1.1.2018-v01).csv"

    handler(
        {
            "input": {
                "husholdninger-med-barn": "raw/green/husholdninger-med-barn/version=1/edition=20190528T133555/Husholdninger_med_barn(1.1.2008-1.1.2018-v01).csv"
            },
            "output": "intermediate/green/boligpriser-blokkleiligheter-status/version=1/edition=20190520T114926/",
            "config": {"type": "status"},
        },
        {},
    )
