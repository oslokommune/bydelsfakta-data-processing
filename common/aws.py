import json
import os
from datetime import date
from dataplatform.awslambda.logging import log_add

import boto3
import numpy as np
import pandas as pd

s3_bucket = os.environ.get("BUCKET_NAME", "ok-origo-dataplatform-dev")


class AwsError(Exception):
    pass


class EmptyPrefixError(AwsError):
    pass


class MultipleFilesInPrefixError(AwsError):
    pass


def find_s3_key(s3_prefix):
    s3 = boto3.client("s3", region_name="eu-west-1")
    objects = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)

    if not objects or "Contents" not in objects:
        raise EmptyPrefixError(s3_prefix)

    if len(objects["Contents"]) > 1:
        raise MultipleFilesInPrefixError(s3_prefix)

    s3_key = next(iter(objects["Contents"]))["Key"]

    log_add(s3_input_path=s3_key)
    return s3_key


def read_from_s3(s3_key, date_column="aar", dtype=None):
    if dtype is None:
        dtype = {
            "delbydel_id": object,
            "delbydel_navn": object,
            "bydel_id": object,
            "bydel_navn": object,
        }
    return pd.read_csv(f"s3://{s3_bucket}/{s3_key}", sep=";", dtype=dtype).rename(
        columns={date_column: "date"}
    )


def write_to_intermediate(output_key: str, output_list: list):
    """
    :param output_key: should be an s3 key on the form `intermediate/green/{datasetid}/{version}/{edition}/`
    :param output_list: a list of dictionaries
    :return:
    """
    log_add(s3_bucket=s3_bucket)
    client = boto3.client("s3")
    for output in output_list:
        filename = "{}.json".format(output.get("id") or output["district"])
        client.put_object(
            Body=json.dumps(output, ensure_ascii=False, allow_nan=False),
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
