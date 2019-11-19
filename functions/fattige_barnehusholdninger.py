import logging

from common.aws import read_from_s3, write_to_intermediate
from common.transform import status, historic
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateB
from common.util import get_latest_edition_of, get_min_max_values_and_ratios


logger = logging.getLogger()
logger.setLevel(logging.INFO)

METADATA = {
    "status": Metadata(heading="Lavinntekts husholdninger med barn", series=[]),
    "historisk": Metadata(heading="Lavinntekts husholdninger med barn", series=[]),
}


def handle(event, context):
    """ Assuming we recieve a complete s3 key"""
    s3_key = event["input"]["fattige-husholdninger"]
    output_key = event["output"]
    type_of_ds = event["config"]["type"]
    logger.info(f"Received s3 key: {s3_key}")
    start(s3_key, output_key, type_of_ds)
    return "OK"


def start(key, output_key, type_of_ds):
    df = read_from_s3(s3_key=key, date_column="aar")

    df = df[df["husholdninger_med_barn_under_18_aar"].notnull()]
    df = df[df["husholdninger_med_barn_under_18_aar_eu_skala"].notnull()]

    df["husholdninger_med_barn_under_18_aar_ratio"] = (
        df["husholdninger_med_barn_under_18_aar_eu_skala"] / 100
    )

    if type_of_ds == "historisk":
        df_historic = historic(df)
        create_ds(output_key, TemplateB(), type_of_ds, *df_historic)
    elif type_of_ds == "status":
        df_status = status(df)
        METADATA[type_of_ds].add_scale(
            get_min_max_values_and_ratios(df, "husholdninger_med_barn_under_18_aar")
        )
        create_ds(output_key, TemplateA(), type_of_ds, *df_status)


def create_ds(output_key, template, type_of_ds, df):
    jsonl = Output(
        df=df,
        template=template,
        metadata=METADATA[type_of_ds],
        values=["husholdninger_med_barn_under_18_aar"],
    ).generate_output()
    write_to_intermediate(output_key=output_key, output_list=jsonl)


if __name__ == "__main__":
    handle(
        {
            "input": {
                "fattige-husholdninger": get_latest_edition_of("fattige-husholdninger")
            },
            "output": "intermediate/green/fattige-barnehusholdninger-status/version=1/edition=20191111T181010/",
            "config": {"type": "status"},
        },
        {},
    )
