from aws_xray_sdk.core import patch_all, xray_recorder
from dataplatform.awslambda.logging import logging_wrapper

import common.aws as common_aws
import common.transform as transform
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateC
from common.util import get_min_max_values_and_ratios
from common.event import event_handler

patch_all()

S3_KEY = "eieform"

METADATA = Metadata(
    heading="Husholdninger fordelt etter eie-/leieforhold",
    series=[
        {"heading": "Leier", "subheading": ""},
        {"heading": "Andels-/aksjeeier", "subheading": ""},
        {"heading": "Selveier", "subheading": ""},
    ],
)


@logging_wrapper("eieform")
@xray_recorder.capture("event_handler")
@event_handler(df=S3_KEY)
def start(df, output_prefix, type_of_ds):
    df = df.rename(
        columns={
            "leier_alle": "leier",
            "borettslag_andel_alle": "andel",
            "selveier_alle": "selveier",
        }
    ).drop(
        [
            "borettslag_andel_uten_studenter",
            "selveier_uten_studenter",
            "leier_uten_studenter",
        ],
        axis=1,
    )

    df = df.drop(df[df["bydel_id"] == "15000"].index)
    df = df.drop(df[df["bydel_id"] == "20000"].index)

    df = df.drop_duplicates()

    df["leier_ratio"] = df["leier"].div(100).round(2)
    df["andel_ratio"] = df["andel"].div(100).round(2)
    df["selveier_ratio"] = df["selveier"].div(100).round(2)

    status = transform.status(df)
    historic = transform.historic(df)

    if type_of_ds == "historisk":
        create_ds(output_prefix, TemplateC(), *historic)
    elif type_of_ds == "status":
        METADATA.add_scale(get_min_max_values_and_ratios(df, "leier"))
        create_ds(output_prefix, TemplateA(), *status)


def create_ds(output_key, template, df):
    jsonl = Output(
        df=df,
        template=template,
        metadata=METADATA,
        values=["leier", "andel", "selveier"],
    ).generate_output()
    common_aws.write_to_intermediate(output_key=output_key, output_list=jsonl)
