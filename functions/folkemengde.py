import numpy as np
import pandas as pd
from aws_xray_sdk.core import patch_all, xray_recorder
from dataplatform.awslambda.logging import logging_wrapper
from enum import Enum

from common.aws import write_to_intermediate
from common.output import Output, Metadata
from common.templates import TemplateB, TemplateG
from common.population_utils import generate_population_df
from common.transform import historic, status
from common.aggregateV2 import Aggregate
from common.aggregation import sum_nans
from common.event import event_handler

patch_all()


class DatasetType(Enum):
    HISTORIC = "historisk"
    HISTORIC_CHANGE = "historisk-prosent"
    KEYNUMBERS = "nokkeltall"


METADATA = {
    DatasetType.HISTORIC: Metadata(
        heading="Folkemengde utvikling historisk", series=[]
    ),
    DatasetType.HISTORIC_CHANGE: Metadata(
        heading="Folkemengde utvikling historisk prosent", series=[]
    ),
    DatasetType.KEYNUMBERS: Metadata(
        heading="Nøkkeltall om befolkningen",
        series=[
            {"heading": "Folkemengde", "subheading": "(totalt)"},
            {"heading": "Utvikling siste år", "subheading": False},
            {"heading": "Utvikling siste 10 år", "subheading": False},
        ],
    ),
}


def filter_10year_set(df):
    max_year = df["date"].max()
    min_year = max_year - 10

    if not (df.date == min_year).any():
        raise ValueError(
            f"Dataset does not contain 10 year before {max_year}: {min_year}"
        )

    year_filter = df["date"].isin([max_year, min_year])
    return df[year_filter]


def calculate_change(df, *, column_name):
    indexed = df.set_index(["bydel_id", "delbydel_id", "date"])
    grouped = indexed.groupby(level="delbydel_id")
    return grouped.diff().rename(columns={"population": column_name}).reset_index()


def calculate_change_ratio(df):
    df = df.sort_values(["bydel_id", "delbydel_id", "date"])
    df["change_ratio"] = df["change"] / df["population"].shift(1)
    df["change_10y_ratio"] = df["change_10y"] / df["population"].shift(10)
    return df


def generate(df):
    df = generate_population_df(df)
    change = calculate_change(df, column_name="change")
    change_10y = calculate_change(filter_10year_set(df), column_name="change_10y")

    join_on = ["date", "bydel_id", "delbydel_id"]
    df = pd.merge(df, change, how="left", on=join_on)
    df = pd.merge(df, change_10y, how="left", on=join_on)

    df = Aggregate(
        {"population": "sum", "change": sum_nans, "change_10y": sum_nans}
    ).aggregate(df)

    df = calculate_change_ratio(df)
    return df


def generate_keynumbers(df):
    [status_df] = status(df)
    growth_df = df[df.date > df.date.max() - 10][
        ["bydel_id", "delbydel_id", "date", "population"]
    ]

    # Avoid dropping NaN indexes during pivot
    growth_df["delbydel_id"] = growth_df["delbydel_id"].fillna("#")

    growth_df = growth_df.pivot_table(
        index=["bydel_id", "delbydel_id"], columns="date", values="population"
    ).reset_index()

    # Add back NaN delbydel_id
    growth_df["delbydel_id"] = growth_df["delbydel_id"].replace("#", np.nan)

    return pd.merge(status_df, growth_df, how="outer", on=["bydel_id", "delbydel_id"])


@logging_wrapper("folkemengde")
@xray_recorder.capture("event_handler")
@event_handler(df="befolkning-etter-kjonn-og-alder")
def start(df, output_prefix, type_of_ds):
    [df] = historic(df)
    df = generate(df)

    if type_of_ds is DatasetType.HISTORIC:
        output = Output(
            values=["population"],
            df=df,
            template=TemplateB(),
            metadata=METADATA[type_of_ds],
        )
    elif type_of_ds is DatasetType.HISTORIC_CHANGE:
        df = df.dropna(axis=0, how="any", subset=["change", "change_ratio"])

        output = Output(
            values=["change"],
            df=df,
            template=TemplateB(),
            metadata=METADATA[type_of_ds],
        )
    elif type_of_ds is DatasetType.KEYNUMBERS:
        df = generate_keynumbers(df)
        max_year = df["date"].max()
        year_range = list(range(max_year - 9, max_year + 1))

        output = Output(
            values=["population", "change", "change_10y"],
            df=df,
            template=TemplateG(history_columns=year_range),
            metadata=METADATA[type_of_ds],
        )

    jsonl = output.generate_output()
    write_to_intermediate(output_key=output_prefix, output_list=jsonl)
