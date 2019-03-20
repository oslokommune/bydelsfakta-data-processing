import json
from datetime import date

import boto3
import pandas as pd

s3_bucket = "ok-origo-dataplatform-dev"


def read_from_s3(s3_key, value_column, date_column="År", dtypes=None):
    if dtypes is None:
        dtypes = {'delbydelid': object}
    return pd.read_csv(f's3://{s3_bucket}/{s3_key}', sep=';', dtypes=dtypes).rename(
            columns={value_column: 'value', date_column: 'date'})


def write_to_intermediate(output_key: str, output_list: list, heading: str, series: list):
    """
    :param output_key: should be an s3 key on the form `intermediate/green/{datasetid}/{version}/{edition}/`
    :param output_list: a list of dictionaries
    :param heading: str
    :param series: a list of dictionaries on the form : `{ 'heading': ... , 'subheading': ... } `
    :return:
    """
    client = boto3.client('s3')

    for output in output_list:
        filename = "{}.json".format(output['district'])
        body = output
        body['meta'] = _metadata(heading=heading, series=series)
        client.put_object(Body=json.dumps(body, ensure_ascii=False),
                          Bucket=s3_bucket,
                          Key=f"{output_key}{filename}")


def _metadata(heading: str,
              series: list,
              help="Dette er en beskrivelse for hvordan dataene leses"):
    return {
        "scope": "bydel",
        "heading": heading,
        "help": help,
        "series": series,
        "publishedDate": date.today().isoformat()
    }
