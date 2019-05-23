import numpy as np
from pprint import pprint
from enum import Enum
from common.aws import read_from_s3, write_to_intermediate
from common.transform import add_district_id
from common.aggregate_dfs import aggregate_from_subdistricts, merge_dfs
from common.transform_output import generate_output_list


class DatasetType(Enum):
    HISTORIC = "historic"
    HISTORIC_CHANGE = "historic_change"
    KEYNUMBERS = "keynumbers"


METADATA = {
    DatasetType.HISTORIC: {"heading": "Folkemengde utvikling historisk", "series": []},
    DatasetType.HISTORIC_CHANGE: {
        "heading": "Folkemengde utvikling historisk prosent",
        "series": [],
    },
    DatasetType.KEYNUMBERS: {
        "heading": "Nøkkeltall om befolkningen",
        "series": [
            {"heading": "Folkemengde", "subheading": "(totalt)"},
            {"heading": "Utvikling siste år", "subheading": False},
            {"heading": "Utvikling siste 10 år", "subheading": False},
        ],
    },
}


def read_population_data(key):
    df = read_from_s3(key)
    df["delbydelid"].fillna("0301999999", inplace=True)
    df = add_district_id(df)
    df = df.rename(
        columns={
            "Antall personer": "value",
            # "Delbydel": "delbydel_id",
            # "district": "bydel_id",
        }
    )
    return df


def population_sum(df):
    return df.groupby(["district", "delbydelid", "date"])["value"].sum().reset_index()


def calculate_change(df):
    indexed = df.set_index(["district", "delbydelid", "date"])
    grouped = indexed.groupby(level="delbydelid")
    change = grouped.diff().rename(columns={"value": "change"}).reset_index()
    change_10y = (
        grouped.diff(periods=10).rename(columns={"value": "change_10y"}).reset_index()
    )

    merge_df = merge_dfs(df, change)
    return merge_dfs(merge_df, change_10y)


def calculate_change_ratio(df):
    df["change_ratio"] = df["change"] / df["value"].shift(1)
    df["change_10y_ratio"] = df["change_10y"] / df["value"].shift(10)
    return df


def sum_nans(df):
    """
    Sum aggregation function with special NaN handling. Only accepts series
    that are either all NaN or contains no NaNs.

    Args:
        df: pandas series.

    Returns:
        sum of the series if all values are not NaN, or NaN if all values in
        the series are NaN.

    Raises:
        ValueError: If the series contains a mix of values and NaNs.
    """
    no_nans = df.notna().all()
    all_nans = df.isna().all()

    if no_nans:
        return np.sum(df)
    elif all_nans:
        return np.nan
    else:
        raise ValueError("Mix of NaN and values")


def calculate(*, key, dataset_type):
    df = read_population_data(key)
    df = population_sum(df)
    df = calculate_change(df)
    df = aggregate_from_subdistricts(
        df,
        [
            {"agg_func": "sum", "data_points": "value"},
            {"agg_func": sum_nans, "data_points": "change"},
            {"agg_func": sum_nans, "data_points": "change_10y"},
        ],
    )
    df = calculate_change_ratio(df)

    if dataset_type is DatasetType.HISTORIC:
        return generate_output_list(df, template="b", data_points=["value"])
    if dataset_type is DatasetType.HISTORIC_CHANGE:
        return generate_output_list(
            df.dropna(axis=0, how="any", subset=["change", "change_ratio"]),
            template="b",
            data_points=["change"],
        )
    if dataset_type is DatasetType.KEYNUMBERS:
        return generate_output_list(
            df, template="g", data_points=["value", "change", "change_10y", "value"]
        )


def handle(event, context):
    dataset_type = DatasetType(event["config"]["type"])
    metadata = METADATA[dataset_type]

    jsonl = calculate(
        key=event["input"]["Befolkningen_etter_bydel_delby-J7khG"],
        dataset_type=dataset_type,
    )
    write_to_intermediate(
        output_key=event["output"],
        heading=metadata["heading"],
        series=metadata["series"],
        output_list=jsonl,
    )


if __name__ == "__main__":
    data = calculate(
        key="raw/green/Befolkningen_etter_bydel_delby-J7khG/version=1-HFe342Fu/edition=EDITION-MHjs3/Befolkningen_etter_bydel_delbydel_kjonn_og_1-aars_aldersgrupper(1.1.2008-1.1.2018-v01).csv",
        dataset_type=DatasetType.KEYNUMBERS,
    )
    pprint(data)
