from aws_xray_sdk.core import patch_all, xray_recorder
from dataplatform.awslambda.logging import logging_wrapper

import common.aws
import common.transform
import common.transform_output
import common.util
from common.util import get_min_max_values_and_ratios
from common import transform
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateB
from common.event import event_handler

patch_all()


@logging_wrapper("levekaar_dodsrate")
@xray_recorder.capture("event_handler")
@event_handler(df="dodsrater")
def start(df, output_prefix, type_of_ds):
    metadata = Metadata(
        heading="Dødelighet (gj.snitt siste 7 år) for personer 55–79 år", series=[]
    )

    df["dodsrate_ratio"] = df["dodsrate"] / 100

    if type_of_ds == "status":
        template = TemplateA()
        df = transform.status(df)[0]
        metadata.add_scale(get_min_max_values_and_ratios(df, "dodsrate"))
    elif type_of_ds == "historisk":
        template = TemplateB()
        df = transform.historic(df)[0]

    output = Output(values=["dodsrate"], df=df, template=template, metadata=metadata)
    jsonl = output.generate_output()
    common.aws.write_to_intermediate(output_key=output_prefix, output_list=jsonl)
