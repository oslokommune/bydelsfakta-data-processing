import json
from datetime import date

import boto3
import pandas as pd
import numpy as np

s3_bucket = "ok-origo-dataplatform-dev"


def read_from_s3(s3_key, date_column="År", dtype=None):
    if dtype is None:
        dtype = {"delbydelid": object}
    return pd.read_csv(f"s3://{s3_bucket}/{s3_key}", sep=";", dtype=dtype).rename(
        columns={date_column: "date"}
    )


def write_to_intermediate(
    output_key: str, output_list: list, heading: str, series: list
):
    """
    :param output_key: should be an s3 key on the form `intermediate/green/{datasetid}/{version}/{edition}/`
    :param output_list: a list of dictionaries
    :param heading: str
    :param series: a list of dictionaries on the form : `{ 'heading': ... , 'subheading': ... } `
    :return:
    """
    client = boto3.client("s3")

    for output in output_list:
        filename = "{}.json".format(output["id"] or output["district"])
        body = output
        body["meta"] = _metadata(heading=heading, series=series)
        client.put_object(
            Body=json.dumps(body, ensure_ascii=False, allow_nan=False, default=default),
            Bucket=s3_bucket,
            Key=f"{output_key}{filename}",
        )


def _metadata(
    heading: str, series: list, help="Dette er en beskrivelse for hvordan dataene leses"
):
    return {
        "scope": "bydel",
        "heading": heading,
        "help": help,
        "series": series,
        "publishedDate": date.today().isoformat(),
    }


def default(o):
    if isinstance(o, np.integer):
        return int(o)
    raise TypeError
