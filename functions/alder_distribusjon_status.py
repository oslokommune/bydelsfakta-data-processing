from aws_xray_sdk.core import patch_all, xray_recorder
from dataplatform.awslambda.logging import logging_wrapper

import common.aggregate_dfs
import common.aws
import common.transform
import common.transform_output
import common.util
from common.aggregateV2 import Aggregate
from common.output import Output, Metadata
from common.templates import TemplateE
from common.event import event_handler

patch_all()
S3_KEY = "befolkning-etter-kjonn-og-alder"


@logging_wrapper("alder_distribusjon_status")
@xray_recorder.capture("event_handler")
@event_handler(df=S3_KEY)
def start(df, output_key, type_of_ds="status"):
    agg = Aggregate("sum")
    df = common.transform.status(df)[0]
    df["total"] = df.loc[:, "0":"120"].sum(axis=1)
    aggregated = agg.aggregate(df, extra_groupby_columns=["kjonn"])
    create_ds(aggregated, output_key)


def _ratio(df):
    df["ratio"] = df["value"] / df["value"].sum()
    return df


def create_ds(df, output_key):
    heading = "Aldersdistribusjon fordelt på kjønn"
    series = [{"heading": "Aldersdistribusjon fordelt på kjønn", "subheading": ""}]

    ages = [str(i) for i in range(0, 121)]
    # To json : convert df to list of json objects
    jsonl = Output(
        df=df,
        template=TemplateE(),
        metadata=Metadata(heading=heading, series=series),
        values=ages,
    ).generate_output()
    common.aws.write_to_intermediate(output_list=jsonl, output_key=output_key)
