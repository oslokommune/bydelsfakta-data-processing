import os

import common.aws as common_aws
import common.transform as transform
from common.transform_output import generate_output_list

os.environ["METADATA_API_URL"] = ""

s3_bucket = "ok-origo-dataplatform-dev"


def handle(event, context):
    s3_key = event["input"]["eieform"]
    output_key = event["output"]
    type_of_ds = event["config"]["type"]
    start(s3_key, output_key, type_of_ds)
    return "OK"


def start(key, output_key, type_of_ds):
    df = common_aws.read_from_s3(s3_key=key, date_column="aar", dtype={"bydel_id": object, "delbydel_id": object})\
        .rename(columns=
                         {
                             "aar": "date",
                             "leier_alle": "leier",
                             "borettslag_andel_alle": "andel",
                             "selveier_alle": "selveier",
                             "bydel_id": "district",
                             "delbydel_id": "delbydelid"
                         })\
        .drop(['borettslag_andel_uten_studenter', 'selveier_uten_studenter', 'leier_uten_studenter'], axis=1)

    df = df.drop(df[df["district"] == "15000"].index)
    df = df.drop(df[df["district"] == "20000"].index)

    df["leier_ratio"] = df["leier"].div(100).round(2)
    df["andel_ratio"] = df["andel"].div(100).round(2)
    df["selveier_ratio"] = df["selveier"].div(100).round(2)

    status = transform.status(df)
    historic = transform.historic(df)

    if type_of_ds == "historisk":
        create_ds(output_key, "c", *historic)
    elif type_of_ds == "status":
        create_ds(output_key, "a", *status)


def create_ds(output_key, template, df):
    heading = "Husholdninger fordelt etter eie-/leieforhold"
    series = [
        {"heading": "Andels-/aksjeeier", "subheading": ""},
        {"heading": "Selveier", "subheading": ""},
        {"heading": "Leier", "subheading": ""},
    ]

    jsonl = generate_output_list(df, template, ["selveier", "andel", "leier"])
    common_aws.write_to_intermediate(output_key=output_key, heading=heading, series=series, output_list=jsonl)


if __name__ == "__main__":
    handle(
        {
            "input": {
                "eieform": "raw/green/eieform/version=1/edition=20190527T101424/Eieform(2015-2017-v01).csv",
            },
            "output": "intermediate/green/eieform-status/version=1/edition=20190524T114926/",
            "config": {"type": "status"},
        },
        {}
    )
