from aws_xray_sdk.core import patch_all, xray_recorder
from dataplatform.awslambda.logging import logging_wrapper

from common.aws import write_to_intermediate
from common.transform import status, historic
from common.aggregateV2 import Aggregate
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateC, TemplateB
from common.util import get_min_max_values_and_ratios
from common.event import event_handler

patch_all()

S3_KEY = "boligmengde-etter-boligtype"

METADATA = {
    "blokk_status": Metadata(heading="Blokker og leiegårder", series=[]),
    "enebolig_status": Metadata(heading="Eneboliger", series=[]),
    "rekkehus_status": Metadata(heading="Rekkehus/tomannsboliger", series=[]),
    "blokk_historisk": Metadata(heading="Blokker og leiegårder", series=[]),
    "enebolig_historisk": Metadata(heading="Eneboliger", series=[]),
    "rekkehus_historisk": Metadata(heading="Rekkehus/tomannsboliger", series=[]),
    "alle_status": Metadata(
        heading="Boliger etter bygningstype",
        series=[
            {"heading": "Blokk, leiegård o.l", "subheading": ""},
            {"heading": "Rekkehus, tomannsboliger o.l", "subheading": ""},
            {"heading": "Enebolig", "subheading": ""},
        ],
    ),
    "alle_historisk": Metadata(
        heading="Boliger etter bygningstype",
        series=[
            {"heading": "Blokk, leiegård o.l", "subheading": ""},
            {"heading": "Rekkehus, tomannsboliger o.l", "subheading": ""},
            {"heading": "Enebolig", "subheading": ""},
            {"heading": "Totalt", "subheading": ""},
        ],
    ),
    "totalt_status": Metadata(heading="Antall boliger", series=[]),
    "totalt_historisk": Metadata(heading="Antall boliger", series=[]),
}


@logging_wrapper("bygningstyper")
@xray_recorder.capture("event_handler")
@event_handler(df=S3_KEY)
def start(df, output_prefix, type_of_ds):
    df = df.rename(
        columns={
            "frittliggende_enebolig_eller_vaaningshus": "enebolig",
            "horisontaldelt_tomannsbolig_eller_annet_boligbygg_med_mindre_enn_3_etasjer": "rekkehus_horisontal",
            "hus_i_kjede_rekkehus_terrasse_hus_vertikaldelt_tomannsbolig": "rekkehus_vertikal",
            "forretningsgaard_bygg_for_felleshusholdning_el": "forretningsbygg",
        }
    )

    df["rekkehus"] = df["rekkehus_horisontal"] + df["rekkehus_vertikal"]
    df["blokk"] = df["blokk_leiegaard_el"] + df["forretningsbygg"]
    df = df.drop(["rekkehus_vertikal", "rekkehus_horisontal"], axis=1)

    df["total"] = df["rekkehus"] + df["blokk"] + df["enebolig"]

    agg = Aggregate(
        {"total": "sum", "rekkehus": "sum", "enebolig": "sum", "blokk": "sum"}
    )

    df = agg.aggregate(df)
    df = agg.add_ratios(df, ["rekkehus", "blokk", "enebolig", "total"], ["total"])

    df_status = status(df)
    df_historic = historic(df)

    if type_of_ds == "alle_status":
        create_ds(
            output_prefix,
            TemplateA(),
            ["blokk", "rekkehus", "enebolig"],
            METADATA[type_of_ds],
            *df_status,
        )
    elif type_of_ds == "alle_historisk":
        create_ds(
            output_prefix,
            TemplateC(),
            ["blokk", "rekkehus", "enebolig", "total"],
            METADATA[type_of_ds],
            *df_historic,
        )
    elif type_of_ds == "blokk_status":
        METADATA[type_of_ds].add_scale(get_min_max_values_and_ratios(df, "blokk"))
        create_ds(
            output_prefix, TemplateA(), ["blokk"], METADATA[type_of_ds], *df_status
        )
    elif type_of_ds == "blokk_historisk":
        create_ds(
            output_prefix, TemplateB(), ["blokk"], METADATA[type_of_ds], *df_historic
        )
    elif type_of_ds == "enebolig_status":
        METADATA[type_of_ds].add_scale(get_min_max_values_and_ratios(df, "enebolig"))
        create_ds(
            output_prefix, TemplateA(), ["enebolig"], METADATA[type_of_ds], *df_status
        )
    elif type_of_ds == "enebolig_historisk":
        create_ds(
            output_prefix, TemplateB(), ["enebolig"], METADATA[type_of_ds], *df_historic
        )
    elif type_of_ds == "rekkehus_status":
        METADATA[type_of_ds].add_scale(get_min_max_values_and_ratios(df, "rekkehus"))
        create_ds(
            output_prefix, TemplateA(), ["rekkehus"], METADATA[type_of_ds], *df_status
        )
    elif type_of_ds == "rekkehus_historisk":
        create_ds(
            output_prefix, TemplateB(), ["rekkehus"], METADATA[type_of_ds], *df_historic
        )
    elif type_of_ds == "totalt_status":
        METADATA[type_of_ds].add_scale(get_min_max_values_and_ratios(df, "totalt"))
        create_ds(
            output_prefix, TemplateA(), ["total"], METADATA[type_of_ds], *df_status
        )
    elif type_of_ds == "totalt_historisk":
        create_ds(
            output_prefix, TemplateB(), ["total"], METADATA[type_of_ds], *df_historic
        )


def create_ds(output_key, template, values, metadata, df):
    jsonl = Output(
        df=df, template=template, metadata=metadata, values=values
    ).generate_output()
    write_to_intermediate(output_key=output_key, output_list=jsonl)
