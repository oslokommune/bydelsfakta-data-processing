import os

import common.aws as common_aws
import common.transform as transform
from common.transform_output import generate_output_list

os.environ["METADATA_API_URL"] = ""

s3_bucket = "ok-origo-dataplatform-dev"


def handle(event, context):
    s3_key = event["input"]["eierform"]
    output = event["output"]
    start(s3_key, output)
    return "OK"


def start(key, output):
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

    df = df.drop(df[df["district"] == "10000"].index)
    df = df.drop(df[df["district"] == "15000"].index)
    df = df.drop(df[df["district"] == "20000"].index)

    df["leier_ratio"] = df["leier"].div(100).round(2)
    df["andel_ratio"] = df["andel"].div(100).round(2)
    df["selveier_ratio"] = df["selveier"].div(100).round(2)

    status = transform.status(df)
    historic = transform.historic(df)

    create_ds(output["status"], "a", *status)
    create_ds(output["historic"], "c", *historic)


def create_ds(output_key, template, df):
    heading = "Eieform"
    series = [
        {"heading": "Borettslag med andelseiere", "subheading": ""},
        {"heading": "Selveiere", "subheading": ""},
        {"heading": "Leiere", "subheading": ""},
    ]

    jsonl = generate_output_list(df, template, ["selveier", "andel", "leier"])
    common_aws.write_to_intermediate(output_key=output_key, heading=heading, series=series, output_list=jsonl)


if __name__ == "__main__":
    handle(
        {
            "bucket": "ok-origo-dataplatform-dev",
            "input": {
                "eierform": "raw/green/eieform/version=1/edition=20190523T214834/Eieform(2015-2017-v01).csv",
            },
            "output": {
                "status": "intermediate/green/eieform-status/version=1/edition=20190524T114926/",
                "historic": "intermediate/green/eieform_historic/version=1/edition=20190524T114926/"
            }
        },
        {}
    )
