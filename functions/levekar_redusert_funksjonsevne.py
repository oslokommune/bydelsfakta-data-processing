from aws_xray_sdk.core import patch_all, xray_recorder
from dataplatform.awslambda.logging import logging_wrapper

import common.transform as transform
import common.aws as common_aws
from common.util import get_latest_edition_of, get_min_max_values_and_ratios
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateB
from common.event import event_handler

patch_all()

graph_metadata = Metadata(
    heading="Personer fra 16 til 66 år med redusert funksjonsevne",
    series=[{"heading": "Redusert funksjonsevne", "subheading": ""}],
)


@logging_wrapper("levekar_redusert_funksjonsevne__old")
@xray_recorder.capture("handler_old")
def handler_old(event, context):
    s3_key_redusert_funksjonsevne = event["input"]["redusert-funksjonsevne"]
    output_key = event["output"]
    type_of_ds = event["config"]["type"]
    input_df = common_aws.read_from_s3(
        s3_key=s3_key_redusert_funksjonsevne, date_column="aar"
    )
    start(input_df, output_key, type_of_ds)
    return f"Created {output_key}"


@logging_wrapper("levekar_redusert_funksjonsevne")
@xray_recorder.capture("event_handler")
@event_handler(input_df="redusert-funksjonsevne")
def _start(*args, **kwargs):
    start(*args, **kwargs)


def start(input_df, output_prefix, type_of_ds):
    data_point = "antall_personer_med_redusert_funksjonsevne"

    input_df[f"{data_point}_ratio"] = (
        input_df["andel_personer_med_redusert_funksjonsevne"] / 100
    )

    output_list = []
    if type_of_ds == "historisk":
        output_list = output_historic(input_df, [data_point])

    elif type_of_ds == "status":
        output_list = output_status(input_df, [data_point])

    if output_list:
        common_aws.write_to_intermediate(
            output_key=output_prefix, output_list=output_list
        )
    else:
        raise Exception("No data in outputlist")


def output_historic(input_df, data_points):
    [input_df] = transform.historic(input_df)
    output = Output(
        values=data_points, df=input_df, metadata=graph_metadata, template=TemplateB()
    ).generate_output()

    return output


def output_status(input_df, data_points):
    [input_df] = transform.status(input_df)
    graph_metadata.add_scale(
        get_min_max_values_and_ratios(
            input_df, "antall_personer_med_redusert_funksjonsevne"
        )
    )
    output = Output(
        values=data_points, df=input_df, metadata=graph_metadata, template=TemplateA()
    ).generate_output()
    return output


if __name__ == "__main__":
    redusert_funksjonsevne_s3_key = get_latest_edition_of("redusert-funksjonsevne")
    handler_old(
        {
            "input": {"redusert-funksjonsevne": redusert_funksjonsevne_s3_key},
            "output": "intermediate/green/levekar-redusert-funksjonsevne-status/version=1/edition=20191111T144000/",
            "config": {"type": "status"},
        },
        None,
    )
