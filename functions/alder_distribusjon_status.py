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


@logging_wrapper("alder_distribusjon_status__old")
@xray_recorder.capture("handler_old")
def handler_old(event, context):
    s3_key = event["input"][S3_KEY]
    output_key = event["output"]
    befolkning = common.aws.read_from_s3(s3_key=s3_key)
    start(befolkning, output_key)
    return "OK"


@logging_wrapper("alder_distribusjon_status")
@xray_recorder.capture("event_handler")
@event_handler(df=S3_KEY)
def _start(*args, **kwargs):
    start(*args, **kwargs)


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


if __name__ == "__main__":
    handler_old(
        {
            "input": {
                "befolkning-etter-kjonn-og-alder": "raw/yellow/befolkning-etter-kjonn-og-alder/version=1/edition=20190528T123302/Befolkningen_etter_bydel_delbydel_kjonn_og_1-aars_aldersgrupper(1.1.2008-1.1.2019-v01).csv"
            },
            "output": "intermediate/green/alder-distribusjon-status/version=1/edition=20190703T142500/",
        },
        {},
    )
