from aws_xray_sdk.core import patch_all, xray_recorder
from dataplatform.awslambda.logging import logging_wrapper

import common.transform as transform
import common.aws as common_aws
from common.aggregateV2 import Aggregate
from common.util import get_min_max_values_and_ratios
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateB
from common.event import event_handler

patch_all()

graph_metadata = Metadata(
    heading="Personer mellom 30-59 år med lav utdanning",
    series=[
        {"heading": "Kun grunnskole eller ingen utdanning oppgitt", "subheading": ""}
    ],
)


@logging_wrapper("levekaar_lav_utdanning")
@xray_recorder.capture("event_handler")
@event_handler(lav_utdanning_raw="lav-utdanning")
def start(lav_utdanning_raw, output_prefix, type_of_ds):
    data_point = "lav_utdanning"

    input_df = generate_input_df(lav_utdanning_raw, data_point)

    output_list = []
    if type_of_ds == "historisk":
        output_list = output_historic(input_df, [data_point])

    elif type_of_ds == "status":
        graph_metadata.add_scale(get_min_max_values_and_ratios(input_df, data_point))
        output_list = output_status(input_df, [data_point])

    if output_list:
        common_aws.write_to_intermediate(
            output_key=output_prefix, output_list=output_list
        )
    else:
        raise Exception("No data in outputlist")


def generate_input_df(lav_utdanning_df, data_point):
    education_categories = [
        "ingen_utdanning_uoppgitt",
        "grunnskole",
        "videregaende",
        "universitet_hogskole_kort",
        "universitet_hogskole_lang",
    ]

    lav_utdanning_df["total"] = lav_utdanning_df[education_categories].sum(axis=1)
    lav_utdanning_df[data_point] = lav_utdanning_df[
        ["ingen_utdanning_uoppgitt", "grunnskole"]
    ].sum(axis=1)

    aggregations = {data_point: "sum", "total": "sum"}
    aggregator = Aggregate(aggregations)
    input_df = aggregator.aggregate(lav_utdanning_df)

    input_df = aggregator.add_ratios(
        input_df, data_points=[data_point], ratio_of=["total"]
    )
    return input_df


def output_historic(input_df, data_points):
    [input_df] = transform.historic(input_df)
    output = Output(
        values=data_points, df=input_df, metadata=graph_metadata, template=TemplateB()
    ).generate_output()

    return output


def output_status(input_df, data_points):
    [input_df] = transform.status(input_df)
    output = Output(
        values=data_points, df=input_df, metadata=graph_metadata, template=TemplateA()
    ).generate_output()
    return output
