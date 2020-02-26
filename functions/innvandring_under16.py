from aws_xray_sdk.core import patch_all, xray_recorder
from dataplatform.awslambda.logging import logging_wrapper

from common.aws import write_to_intermediate
from common.transform import status, historic
from common.aggregateV2 import Aggregate
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateB
from common.util import get_min_max_values_and_ratios
from common.event import event_handler

patch_all()
S3_KEY = "innvandrer-befolkningen-0-15-ar"

METADATA = {
    "en-innvandrer_historisk": Metadata(
        heading="Personer (under 16) med én innvandrerforelder", series=[]
    ),
    "en-innvandrer_status": Metadata(
        heading="Personer (under 16) med én innvandrerforelder", series=[]
    ),
    "to-innvandrer_historisk": Metadata(
        heading="Personer (under 16) med to innvandrerforeldre", series=[]
    ),
    "to-innvandrer_status": Metadata(
        heading="Personer (under 16) med to innvandrerforeldre", series=[]
    ),
    "innvandrer_status": Metadata(heading="Innvandrere (under 16)", series=[]),
    "innvandrer_historisk": Metadata(heading="Andel innvandrere (under 16)", series=[]),
}

DATA_POINTS = {
    "en-innvandrer_historisk": ["en_forelder"],
    "en-innvandrer_status": ["en_forelder"],
    "to-innvandrer_historisk": ["to_foreldre"],
    "to-innvandrer_status": ["to_foreldre"],
    "innvandrer_status": ["innvandrer"],
    "innvandrer_historisk": ["innvandrer"],
}

VALUE_POINTS = ["to_foreldre", "en_forelder", "innvandrer"]


@logging_wrapper("innvandring_under16")
@xray_recorder.capture("event_handler")
@event_handler(df=S3_KEY)
def start(df, output_prefix, type_of_ds):
    df = df[df["alder"] == "0-15 år"].reset_index()
    df["en_forelder"] = (
        df["norskfodt_med_en_utenlandskfodt_forelder"]
        + df["utenlandsfodt_med_en_norsk_forelder"]
    )
    df = df.rename(columns={"norskfodt_med_innvandrerforeldre": "to_foreldre"})

    df["total"] = (
        df["fodt_i_utlandet_av_norskfodte_foreldre"]
        + df["innvandrer"]
        + df["uten_innvandringsbakgrunn"]
        + df["to_foreldre"]
        + df["en_forelder"]
    )

    agg = Aggregate(
        {
            "to_foreldre": "sum",
            "en_forelder": "sum",
            "innvandrer": "sum",
            "total": "sum",
        }
    )

    df = agg.aggregate(df)
    df = agg.add_ratios(df, VALUE_POINTS, ["total"])

    if type_of_ds == "en-innvandrer_historisk":
        create_ds(output_prefix, TemplateB(), type_of_ds, *historic(df))
    elif type_of_ds == "en-innvandrer_status":
        METADATA[type_of_ds].add_scale(
            get_min_max_values_and_ratios(df, DATA_POINTS[type_of_ds][0])
        )
        create_ds(output_prefix, TemplateA(), type_of_ds, *status(df))
    elif type_of_ds == "to-innvandrer_historisk":
        create_ds(output_prefix, TemplateB(), type_of_ds, *historic(df))
    elif type_of_ds == "to-innvandrer_status":
        METADATA[type_of_ds].add_scale(
            get_min_max_values_and_ratios(df, DATA_POINTS[type_of_ds][0])
        )
        create_ds(output_prefix, TemplateA(), type_of_ds, *status(df))
    elif type_of_ds == "innvandrer_historisk":
        create_ds(output_prefix, TemplateB(), type_of_ds, *historic(df))
    elif type_of_ds == "innvandrer_status":
        METADATA[type_of_ds].add_scale(
            get_min_max_values_and_ratios(df, DATA_POINTS[type_of_ds][0])
        )
        create_ds(output_prefix, TemplateA(), type_of_ds, *status(df))


def create_ds(output_key, template, type_of_ds, df):
    jsonl = Output(
        df=df,
        template=template,
        metadata=METADATA[type_of_ds],
        values=DATA_POINTS[type_of_ds],
    ).generate_output()
    write_to_intermediate(output_key=output_key, output_list=jsonl)
