from aws_xray_sdk.core import patch_all, xray_recorder
from dataplatform.awslambda.logging import logging_wrapper

from common import aws, util
from common.transform import status, historic
from common.aggregateV2 import Aggregate, ColumnNames
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateB, TemplateC
from common.util import get_min_max_values_and_ratios
from common.event import event_handler

patch_all()

METADATA = {
    "alle_status": Metadata(
        heading="Husholdninger med barn",
        series=[
            {"heading": "Husholdninger", "subheading": "med 1 barn"},
            {"heading": "Husholdninger", "subheading": "med 2 barn"},
            {"heading": "Husholdninger", "subheading": "med 3 barn eller flere"},
        ],
    ),
    "alle_historisk": Metadata(
        heading="Husholdninger med barn",
        series=[
            {"heading": "Husholdninger", "subheading": "med 1 barn"},
            {"heading": "Husholdninger", "subheading": "med 2 barn"},
            {"heading": "Husholdninger", "subheading": "med 3 barn eller flere"},
            {"heading": "Totalt", "subheading": ""},
        ],
    ),
    "1barn_status": Metadata(heading="Husholdninger med 1 barn", series=[]),
    "1barn_historisk": Metadata(heading="Husholdninger med 1 barn", series=[]),
    "2barn_status": Metadata(heading="Husholdninger med 2 barn", series=[]),
    "2barn_historisk": Metadata(heading="Husholdninger med 2 barn", series=[]),
    "3barn_status": Metadata(heading="Husholdninger med 3 barn", series=[]),
    "3barn_historisk": Metadata(heading="Husholdninger med 3 barn", series=[]),
}

VALUE_CATEGORY = {
    "alle_status": ["one_child", "two_child", "three_or_more"],
    "alle_historisk": ["one_child", "two_child", "three_or_more", "total"],
    "1barn_status": ["one_child"],
    "1barn_historisk": ["one_child"],
    "2barn_status": ["two_child"],
    "2barn_historisk": ["two_child"],
    "3barn_status": ["three_or_more"],
    "3barn_historisk": ["three_or_more"],
}

DATA_POINTS = ["one_child", "two_child", "three_or_more", "no_children", "total"]

column_names = ColumnNames()


@logging_wrapper("husholdning_med_barn__old")
@xray_recorder.capture("handler_old")
def handler_old(event, context):
    s3_key = event["input"]["husholdninger-med-barn"]
    output_key = event["output"]
    type = event["config"]["type"]
    source = aws.read_from_s3(s3_key=s3_key)
    start(source, output_key, type)
    return "OK"


@logging_wrapper("husholdning_med_barn")
@xray_recorder.capture("event_handler")
@event_handler(source="husholdninger-med-barn")
def _start(*args, **kwargs):
    start(*args, **kwargs)


def start(source, output_prefix, type_of_ds):
    source["one_child"] = source["ett_barn_i_hh"]
    source["two_child"] = source["to_barn_i_hh"]
    source["three_or_more"] = source["tre_barn_i_hh"] + source["fire_barn_eller_mer"]
    source["no_children"] = source["ingen_barn_i_hh"]

    source = source.drop(
        columns=[
            "ett_barn_i_hh",
            "to_barn_i_hh",
            "tre_barn_i_hh",
            "fire_barn_eller_mer",
            "ingen_barn_i_hh",
        ]
    )

    source["total"] = (
        source["one_child"]
        + source["two_child"]
        + source["three_or_more"]
        + source["no_children"]
    )

    agg = Aggregate("sum")

    source = agg.aggregate(source)

    df = agg.add_ratios(source, data_points=DATA_POINTS, ratio_of=["total"])

    if type_of_ds == "alle_status":
        create_ds(output_prefix, TemplateA(), type_of_ds, *status(df))
    elif type_of_ds == "alle_historisk":
        create_ds(output_prefix, TemplateC(), type_of_ds, *historic(df))
    elif type_of_ds == "1barn_status":
        METADATA[type_of_ds].add_scale(get_min_max_values_and_ratios(df, "one_child"))
        create_ds(output_prefix, TemplateA(), type_of_ds, *status(df))
    elif type_of_ds == "1barn_historisk":
        create_ds(output_prefix, TemplateB(), type_of_ds, *historic(df))
    elif type_of_ds == "2barn_status":
        METADATA[type_of_ds].add_scale(get_min_max_values_and_ratios(df, "two_child"))
        create_ds(output_prefix, TemplateA(), type_of_ds, *status(df))
    elif type_of_ds == "2barn_historisk":
        create_ds(output_prefix, TemplateB(), type_of_ds, *historic(df))
    elif type_of_ds == "3barn_status":
        METADATA[type_of_ds].add_scale(
            get_min_max_values_and_ratios(df, "three_or_more")
        )
        create_ds(output_prefix, TemplateA(), type_of_ds, *status(df))
    elif type_of_ds == "3barn_historisk":
        create_ds(output_prefix, TemplateB(), type_of_ds, *historic(df))
    else:
        raise Exception("Wrong dataset type")


def create_ds(output_key, template, type_of_ds, df):
    jsonl = Output(
        df=df,
        template=template,
        metadata=METADATA[type_of_ds],
        values=VALUE_CATEGORY[type_of_ds],
    ).generate_output()
    aws.write_to_intermediate(output_key=output_key, output_list=jsonl)


if __name__ == "__main__":
    handler_old(
        {
            "input": {
                "husholdninger-med-barn": util.get_latest_edition_of(
                    "husholdninger-med-barn"
                )
            },
            "output": "intermediate/green/husholdninger-totalt-status/version=1/edition=20190819T110202/",
            "config": {"type": "3barn_status"},
        },
        {},
    )
