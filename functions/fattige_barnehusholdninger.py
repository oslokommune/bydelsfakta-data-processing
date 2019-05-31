import numpy as np
import logging

from common.aws import read_from_s3, write_to_intermediate
from common.transform import status, historic
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateB
from common.util import get_latest_edition_of


logger = logging.getLogger()
logger.setLevel(logging.INFO)

METADATA = {
    "status": Metadata(heading="Lavinntekts husholdninger med barn", series=[]),
    "historisk": Metadata(heading="Lavinntekts husholdninger med barn", series=[]),
}


def handle(event, context):
    """ Assuming we recieve a complete s3 key"""
    s3_key = event["input"]["fattige-barnehusholdninger"]
    output_key = event["output"]
    type_of_ds = event["config"]["type"]
    logger.info(f"Received s3 key: {s3_key}")
    start(s3_key, output_key, type_of_ds)
    return "OK"


def start(key, output_key, type_of_ds):
    df = read_from_s3(s3_key=key, date_column="aar")

    df["antall_fattige_barnehusholdninger"] = (
        df["husholdninger_med_barn_under_18_aar"]
        * df["husholdninger_med_barn_under_18_aar_eu_skala"]
        / 100
    )

    # FIXME: This should be done in csvlt
    df.loc[df["bydel_navn"] == "Oslo i alt", "bydel_id"] = "00"
    df.loc[df["delbydel_navn"] == "Oslo i alt", "delbydel_navn"] = np.nan

    df["antall_fattige_barnehusholdninger_ratio"] = (
        df["husholdninger_med_barn_under_18_aar_eu_skala"] / 100
    )

    df_status = status(df)
    df_historic = historic(df)

    if type_of_ds == "historisk":
        create_ds(output_key, TemplateB(), type_of_ds, *df_historic)
    elif type_of_ds == "status":
        create_ds(output_key, TemplateA(), type_of_ds, *df_status)


def create_ds(output_key, template, type_of_ds, df):
    jsonl = Output(
        df=df,
        template=template,
        metadata=METADATA[type_of_ds],
        values=["antall_fattige_barnehusholdninger"],
    ).generate_output()
    write_to_intermediate(output_key=output_key, output_list=jsonl)


if __name__ == "__main__":
    handle(
        {
            "input": {
                "fattige-barnehusholdninger": get_latest_edition_of(
                    "fattige-husholdninger"
                )
            },
            "output": "intermediate/green/fattige-barnehusholdninger/version=1/edition=20190531T102550/",
            "config": {"type": "status"},
        },
        {},
    )
