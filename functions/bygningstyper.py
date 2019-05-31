from common.aws import read_from_s3, write_to_intermediate
from common.transform import status, historic
from common.aggregateV2 import Aggregate
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateC, TemplateB
from common.util import get_latest_edition_of

METADATA = {
    "blokk_status": Metadata(heading="Blokker og leiegårder", series=[]),
    "enebolig_status": Metadata(heading="Eneboliger", series=[]),
    "rekkehus_status": Metadata(heading="Rekkehus/tomannsboliger", series=[]),
    "blokk_historisk": Metadata(heading="Blokker og leiegårder", series=[]),
    "enebolig_historisk": Metadata(heading="Eneboliger", series=[]),
    "rekkehus_historisk": Metadata(heading="Rekkehus/tomannsboliger", series=[]),
    "totalt_status": Metadata(
        heading="Boliger etter bygningstype",
        series=[
            {"heading": "Blokk, leiegård o.l", "subheading": ""},
            {"heading": "Rekkehus, tomannsboliger o.l", "subheading": ""},
            {"heading": "Enebolig", "subheading": ""},
        ],
    ),
    "totalt_historisk": Metadata(
        heading="Boliger etter bygningstype",
        series=[
            {"heading": "Blokk, leiegård o.l", "subheading": ""},
            {"heading": "Rekkehus, tomannsboliger o.l", "subheading": ""},
            {"heading": "Enebolig", "subheading": ""},
        ],
    ),
}


def handle(event, context):
    s3_key = event["input"]["boligmengde-etter-boligtype"]
    output_key = event["output"]
    type_of_ds = event["config"]["type"]
    start(s3_key, output_key, type_of_ds)
    return "OK"


def start(key, output_key, type_of_ds):
    df = read_from_s3(s3_key=key, date_column="aar").rename(
        columns={
            "blokk_leiegaard_el": "blokk",
            "frittliggende_enebolig_eller_vaaningshus": "enebolig",
            "horisontaldelt_tomannsbolig_eller_annet_boligbygg_med_mindre_enn_3_etasjer": "rekkehus_horisontal",
            "hus_i_kjede_rekkehus_terrasse_hus_vertikaldelt_tomannsbolig": "rekkehus_vertikal",
            "forretningsgaard_bygg_for_felleshusholdning_el": "forretningsbygg",
        }
    )

    df["rekkehus"] = df["rekkehus_horisontal"] + df["rekkehus_vertikal"]
    df = df.drop(["rekkehus_vertikal", "rekkehus_horisontal"], axis=1)

    df["total"] = df["rekkehus"] + df["blokk"] + df["enebolig"] + df["forretningsbygg"]

    agg = Aggregate(
        {
            "total": "sum",
            "rekkehus": "sum",
            "enebolig": "sum",
            "blokk": "sum",
            "forretningsbygg": "sum",
        }
    )

    df = agg.aggregate(df)
    df = agg.add_ratios(df, ["rekkehus", "blokk", "enebolig"], ["total"])

    df_status = status(df)
    df_historic = historic(df)

    if type_of_ds == "totalt_status":
        create_ds(
            output_key,
            TemplateA(),
            ["blokk", "rekkehus", "enebolig"],
            METADATA[type_of_ds],
            *df_status,
        )
    elif type_of_ds == "totalt_historisk":
        create_ds(
            output_key,
            TemplateC(),
            ["blokk", "rekkehus", "enebolig"],
            METADATA[type_of_ds],
            *df_historic,
        )
    elif type_of_ds == "blokk_status":
        create_ds(output_key, TemplateA(), ["blokk"], METADATA[type_of_ds], *df_status)
    elif type_of_ds == "blokk_historisk":
        create_ds(
            output_key, TemplateB(), ["blokk"], METADATA[type_of_ds], *df_historic
        )
    elif type_of_ds == "enebolig_status":
        create_ds(
            output_key, TemplateA(), ["enebolig"], METADATA[type_of_ds], *df_status
        )
    elif type_of_ds == "enebolig_historisk":
        create_ds(
            output_key, TemplateB(), ["enebolig"], METADATA[type_of_ds], *df_historic
        )
    elif type_of_ds == "rekkehus_status":
        create_ds(
            output_key, TemplateA(), ["rekkehus"], METADATA[type_of_ds], *df_status
        )
    elif type_of_ds == "rekkehus_historisk":
        create_ds(
            output_key, TemplateB(), ["rekkehus"], METADATA[type_of_ds], *df_historic
        )


def create_ds(output_key, template, values, metadata, df):
    jsonl = Output(
        df=df, template=template, metadata=metadata, values=values
    ).generate_output()
    write_to_intermediate(output_key=output_key, output_list=jsonl)


if __name__ == "__main__":
    handle(
        {
            "input": {
                "boligmengde-etter-boligtype": get_latest_edition_of("boligmengde-etter-boligtype")
            },
            "output": "intermediate/green/bygningstyper_rekkehus_status/version=1/edition=20190531T105555/",
            "config": {"type": "rekkehus_status"},
        },
        {},
    )
