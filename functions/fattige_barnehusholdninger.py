from aws_xray_sdk.core import patch_all, xray_recorder
from dataplatform.awslambda.logging import logging_wrapper

from common.aws import write_to_intermediate
from common.transform import status, historic
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateB
from common.util import get_min_max_values_and_ratios
from common.event import event_handler

patch_all()

S3_KEY = "fattige-husholdninger"

METADATA = {
    "status": Metadata(heading="Lavinntekts husholdninger med barn", series=[]),
    "historisk": Metadata(heading="Lavinntekts husholdninger med barn", series=[]),
}


@logging_wrapper("fattige_barnehusholdninger")
@xray_recorder.capture("event_handler")
@event_handler(df=S3_KEY)
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
