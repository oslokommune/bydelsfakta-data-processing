from aws_xray_sdk.core import patch_all, xray_recorder
from dataplatform.awslambda.logging import logging_wrapper

from common import aws, transform
from common.aggregateV2 import Aggregate, ColumnNames
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateC
from common.util import get_min_max_values_and_ratios
from common.event import event_handler

patch_all()

DATA_POINTS_ALL = [
    "aleneboende",
    "par_uten_barn",
    "par_med_barn",
    "mor_far_med_barn",
    "flerfamiliehusholdninger",
    "total",
]

DATA_POINTS_STATUS = [
    "aleneboende",
    "par_uten_barn",
    "par_med_barn",
    "mor_far_med_barn",
    "flerfamiliehusholdninger",
]

column_names = ColumnNames()


@logging_wrapper("husholdningstyper")
@xray_recorder.capture("event_handler")
@event_handler(source="husholdningstyper")
def start(source, output_prefix, type_of_ds):
    source["par_uten_barn"] = source["par_uten_hjemmeboende_barn"]
    source["par_med_barn"] = source["par_med_smaa_barn"] + source["par_med_store_barn"]
    source["mor_far_med_barn"] = (
        source["mor_eller_far_med_smaa_barn"] + source["mor_eller_far_med_store_barn"]
    )
    source["flerfamiliehusholdninger"] = (
        source["enfamiliehusholdninger_med_voksne_barn"]
        + source["flerfamiliehusholdninger_med_smaa_barn"]
        + source["flerfamiliehusholdninger_med_store_barn"]
        + source["flerfamiliehusholdninger_uten_barn_0_til_17_aar"]
    )

    source = source.drop(
        columns=[
            "par_uten_hjemmeboende_barn",
            "par_med_smaa_barn",
            "par_med_store_barn",
            "mor_eller_far_med_smaa_barn",
            "mor_eller_far_med_store_barn",
            "enfamiliehusholdninger_med_voksne_barn",
            "flerfamiliehusholdninger_med_store_barn",
            "flerfamiliehusholdninger_med_smaa_barn",
            "flerfamiliehusholdninger_uten_barn_0_til_17_aar",
        ]
    )

    source["total"] = (
        source["par_uten_barn"]
        + source["par_med_barn"]
        + source["mor_far_med_barn"]
        + source["flerfamiliehusholdninger"]
        + source["aleneboende"]
    )

    source = source.groupby(column_names.default_groupby_columns(), as_index=False).agg(
        "sum"
    )

    agg = Aggregate("sum")

    source = agg.aggregate(source)

    aggregated = agg.add_ratios(source, data_points=DATA_POINTS_ALL, ratio_of=["total"])

    scale = get_min_max_values_and_ratios(aggregated, "aleneboende")

    if type_of_ds == "status":
        [df] = transform.status(aggregated)
        template = TemplateA()
        DATA_POINTS = DATA_POINTS_STATUS
        series = [
            {"heading": "Énpersonshusholdninger", "subheading": ""},
            {"heading": "Par uten barn", "subheading": ""},
            {"heading": "Par med barn", "subheading": ""},
            {"heading": "Mor eller far", "subheading": "med barn"},
            {"heading": "Flerfamiliehusholdninger", "subheading": ""},
        ]

    elif type_of_ds == "historisk":
        [df] = transform.historic(aggregated)
        template = TemplateC()
        DATA_POINTS = DATA_POINTS_ALL
        series = [
            {"heading": "Énpersonshusholdninger", "subheading": ""},
            {"heading": "Par uten barn", "subheading": ""},
            {"heading": "Par med barn", "subheading": ""},
            {"heading": "Mor eller far", "subheading": "med barn"},
            {"heading": "Flerfamiliehusholdninger", "subheading": ""},
            {"heading": "Totalt", "subheading": ""},
        ]

    else:
        raise Exception("Wrong dataset type")

    metadata = Metadata(
        heading="Husholdninger etter husholdningstype", series=series, scale=scale
    )

    output = Output(
        df=df, values=DATA_POINTS, template=template, metadata=metadata
    ).generate_output()

    aws.write_to_intermediate(output_key=output_prefix, output_list=output)
