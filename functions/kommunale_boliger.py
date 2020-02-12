from aws_xray_sdk.core import patch_all, xray_recorder
from dataplatform.awslambda.logging import logging_wrapper

from common.aws import read_from_s3, write_to_intermediate
from common.transform import status, historic
from common.aggregateV2 import Aggregate
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateB
from common.util import get_min_max_values_and_ratios
from common.event import event_handler

patch_all()

METADATA = {
    "status": Metadata(heading="Kommunale boliger av boligmassen i alt", series=[]),
    "historisk": Metadata(heading="Kommunale boliger av boligmassen i alt", series=[]),
}


def housing(key):
    df = read_from_s3(key)
    df["total"] = (
        df["blokk_leiegaard_el"]
        + df["forretningsgaard_bygg_for_felleshusholdning_el"]
        + df["frittliggende_enebolig_eller_vaaningshus"]
        + df[
            "horisontaldelt_tomannsbolig_eller_annet_boligbygg_med_mindre_enn_3_etasjer"
        ]
        + df["hus_i_kjede_rekkehus_terrasse_hus_vertikaldelt_tomannsbolig"]
    )
    return df


def generate(df_municipal, df_housing):
    agg = Aggregate({"antall_boliger": "sum", "total": "sum"})

    df = agg.merge(df_municipal, df_housing)
    df = agg.aggregate(df)
    df = agg.add_ratios(df, ["antall_boliger"], ["total"])
    return df


def start(df_municipal, df_housing, output_prefix, dataset_type):
    if dataset_type == "status":
        dfs = status(df_municipal, df_housing)
        template = TemplateA()
    elif dataset_type == "historisk":
        dfs = historic(df_municipal, df_housing)
        template = TemplateB()

    df = generate(*dfs)
    METADATA[dataset_type].add_scale(
        get_min_max_values_and_ratios(df, "antall_boliger")
    )
    jsonl = Output(
        values=["antall_boliger"],
        df=df,
        template=template,
        metadata=METADATA[dataset_type],
    ).generate_output()
    write_to_intermediate(output_key=output_prefix, output_list=jsonl)


@logging_wrapper("kommunale_boliger__old")
@xray_recorder.capture("handler_old")
def handler_old(event, context):
    dataset_type = event["config"]["type"]
    output_key = event["output"]
    municipal_key = event["input"]["kommunale-boliger"]
    housing_key = event["input"]["boligmengde-etter-boligtype"]

    df_municipal = read_from_s3(municipal_key)
    df_housing = housing(housing_key)

    start(df_municipal, df_housing, output_key, dataset_type)
    return "OK"


@logging_wrapper("kommunale_boliger")
@xray_recorder.capture("event_handler")
@event_handler(
    df_municipal="kommunale-boliger", df_housing="boligmengde-etter-boligtype"
)
def _start(*args, **kwargs):
    start(*args, **kwargs)
