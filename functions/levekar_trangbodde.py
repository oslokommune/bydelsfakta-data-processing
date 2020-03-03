import pandas as pd
from aws_xray_sdk.core import patch_all, xray_recorder
from dataplatform.awslambda.logging import logging_wrapper

import common.aws as common_aws
import common.transform as transform
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateB
from common.util import get_min_max_values_and_ratios
from common.event import event_handler

patch_all()

pd.set_option("display.max_rows", 1000)

graph_metadata = Metadata(
    heading="Levekår Trangbodde", series=[{"heading": "Trangbodde", "subheading": ""}]
)


@logging_wrapper("levekar_trangbodde")
@xray_recorder.capture("event_handler")
@event_handler(trangbodde_raw="trangbodde")
def start(trangbodde_raw, output_prefix, type_of_ds):
    datapoint = "antall_som_bor_trangt"

    trangbodde_raw["antall_som_bor_trangt_ratio"] = (
        trangbodde_raw["andel_som_bor_trangt"] / 100
    )

    output_list = []
    if type_of_ds == "historisk":
        output_list = output_historic(trangbodde_raw, [datapoint])

    elif type_of_ds == "status":
        output_list = output_status(trangbodde_raw, [datapoint])

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
        get_min_max_values_and_ratios(input_df, "antall_som_bor_trangt")
    )
    output = Output(
        values=data_points, df=input_df, metadata=graph_metadata, template=TemplateA()
    ).generate_output()
    return output
