from aws_xray_sdk.core import patch_all, xray_recorder
from dataplatform.awslambda.logging import logging_wrapper

import common.aggregate_dfs
import common.aws
import common.transform
import common.transform_output
from common.aggregateV2 import ColumnNames
from common.output import Metadata, Output
from common.templates import TemplateB, TemplateA
from common.util import get_latest_edition_of, get_min_max_values_and_ratios
from common.event import event_handler

patch_all()

column_names = ColumnNames()

ikke_fullfort_antall = "antall_personer_ikke_fullfort_i_lopet_av_5_aar"
ikke_fullfort_andel = "andelen_som_ikke_har_fullfort_i_lopet_av_5_aar"


def process(df, type):
    df[ikke_fullfort_andel] = df[ikke_fullfort_andel] / 100
    df = df.rename(columns={ikke_fullfort_andel: f"{ikke_fullfort_antall}_ratio"})

    metadata = Metadata(
        heading="Personer som ikke har fullført vgs",
        series=[
            {
                "Heading": "Antall personer, ikke fullført i løpet av 5 år",
                "subheading": "",
            }
        ],
    )
    if type == "historisk":
        [df] = common.transform.historic(df)
        template = TemplateB()
    elif type == "status":
        [df] = common.transform.status(df)
        metadata.add_scale(get_min_max_values_and_ratios(df, ikke_fullfort_antall))
        template = TemplateA()
    else:
        raise Exception("type must be status or historisk")

    output = Output(
        df=df, values=[ikke_fullfort_antall], metadata=metadata, template=template
    ).generate_output()

    return output


def write(output, s3_key):
    common.aws.write_to_intermediate(output_list=output, output_key=s3_key)
    return s3_key


@logging_wrapper("ikke_fullfort_vgs__old")
@xray_recorder.capture("handler_old")
def handler_old(event, context):
    s3_key = event["input"]["ikke-fullfort-vgs"]
    output_key = event["output"]
    type_of_ds = event["config"]["type"]
    df = common.aws.read_from_s3(s3_key)
    start(df, output_key, type_of_ds)
    return output_key


@logging_wrapper("ikke_fullfort_vgs")
@xray_recorder.capture("event_handler")
@event_handler(df="ikke-fullfort-vgs")
def _start(*args, **kwargs):
    start(*args, **kwargs)


def start(df, output_prefix, type_of_ds):
    output = process(df, type_of_ds)
    write(output, output_prefix)


if __name__ == "__main__":
    handler_old(
        {
            "input": {"ikke-fullfort-vgs": get_latest_edition_of("ikke-fullfort-vgs")},
            "output": "intermediate/green/ikke-fullfort-vgs-status/version=1/edition=20110531T102550/",
            "config": {"type": "status"},
        },
        {},
    )
