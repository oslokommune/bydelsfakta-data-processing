from aws_xray_sdk.core import patch_all, xray_recorder
from dataplatform.awslambda.logging import logging_wrapper

from common.aws import read_from_s3, write_to_intermediate
from common.transform import status, historic
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateB
from common.util import get_latest_edition_of, get_min_max_values_and_ratios
from common.event import event_handler

patch_all()

S3_KEY = "fattige-husholdninger"

METADATA = {
    "status": Metadata(heading="Lavinntekts husholdninger med barn", series=[]),
    "historisk": Metadata(heading="Lavinntekts husholdninger med barn", series=[]),
}


@logging_wrapper("fattige_barnehusholdninger__old")
@xray_recorder.capture("handler_old")
def handler_old(event, context):
    s3_key = event["input"]["fattige-husholdninger"]
    output_key = event["output"]
    type_of_ds = event["config"]["type"]
    df = read_from_s3(s3_key=s3_key, date_column="aar")
    start(df, output_key, type_of_ds)
    return "OK"


@logging_wrapper("fattige_barnehusholdninger")
@xray_recorder.capture("event_handler")
@event_handler(df=S3_KEY)
def _start(*args, **kwargs):
    start(*args, **kwargs)


def start(df, output_prefix, type_of_ds):
    df = df[df["husholdninger_med_barn_under_18_aar_eu_skala_antall"].notnull()]
    df = df[df["husholdninger_med_barn_under_18_aar_eu_skala_andel"].notnull()]

    df["husholdninger_med_barn_under_18_aar_eu_skala"] = df[
        "husholdninger_med_barn_under_18_aar_eu_skala_antall"
    ]
    df["husholdninger_med_barn_under_18_aar_eu_skala_ratio"] = (
        df["husholdninger_med_barn_under_18_aar_eu_skala_andel"] / 100
    )

    if type_of_ds == "historisk":
        df_historic = historic(df)
        create_ds(output_prefix, TemplateB(), type_of_ds, *df_historic)
    elif type_of_ds == "status":
        df_status = status(df)
        METADATA[type_of_ds].add_scale(
            get_min_max_values_and_ratios(
                df, "husholdninger_med_barn_under_18_aar_eu_skala"
            )
        )
        create_ds(output_prefix, TemplateA(), type_of_ds, *df_status)


def create_ds(output_key, template, type_of_ds, df):
    jsonl = Output(
        df=df,
        template=template,
        metadata=METADATA[type_of_ds],
        values=["husholdninger_med_barn_under_18_aar_eu_skala"],
    ).generate_output()
    write_to_intermediate(output_key=output_key, output_list=jsonl)


if __name__ == "__main__":
    handler_old(
        {
            "input": {
                "fattige-husholdninger": get_latest_edition_of("fattige-husholdninger")
            },
            "output": "intermediate/green/fattige-barnehusholdninger-status/version=1/edition=20200106T181010/",
            "config": {"type": "status"},
        },
        {},
    )
