from aws_xray_sdk.core import patch_all, xray_recorder
from dataplatform.awslambda.logging import logging_wrapper

import common.aws
import common.transform
import common.transform_output
import common.util
from common.output import Output, Metadata
from common.templates import TemplateA
from common.util import get_min_max_values_and_ratios
from common.event import event_handler

patch_all()


@logging_wrapper("boligpriser")
@xray_recorder.capture("event_handler")
@event_handler(df="boligpriser-blokkleiligheter")
def start(df, output_prefix, type_of_ds):
    df = df.rename(columns={"kvmpris": "value"})
    df = df.drop(columns=["antall_omsatte_blokkleiligheter"])

    # TODO: Fix in csvlt, missing id for Oslo i alt
    df.loc[df["bydel_navn"] == "Oslo i alt", "bydel_id"] = "00"

    if type_of_ds == "status":
        df = common.transform.status(df)
    elif type_of_ds == "historisk":
        df = common.transform.historic(df)
    else:
        raise Exception("Wrong dataset type. Use 'status' or 'historisk'.")

    json_lines = generate(*df, type_of_ds)

    common.aws.write_to_intermediate(output_prefix, json_lines)


def generate(df, ds_type):
    heading = "Gjennomsnittpris (kr) pr kvm for blokkleilighet"
    series = [
        {"heading": "Gjennomsnittpris (kr) pr kvm for blokkleilighet", "subheading": ""}
    ]
    scale = get_min_max_values_and_ratios(df, "value")

    # To json : convert df to list of json objects
    jsonl = Output(
        df=df[~df["value"].isna()],
        values=["value"],
        template=TemplateA(),
        metadata=Metadata(heading=heading, series=series, scale=scale),
    ).generate_output()
    return jsonl
