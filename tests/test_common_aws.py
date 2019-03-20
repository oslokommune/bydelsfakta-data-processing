import json

import boto3
from moto import mock_s3

import common.aws as aws


@mock_s3
def test_write_files():
    bucket_name = "ok-origo-dataplatform-dev"
    partial_key = "intermediate/some/value/here/"
    client = boto3.client("s3")
    client.create_bucket(Bucket=bucket_name)
    output_list = [{'district': "01"}, {'district': "02"}, {'district': "03"}]
    series = [{'heading': 'heading', 'subheading': 'subheading'}]
    aws.write_to_intermediate(partial_key, output_list, heading="Test", series=series)
    obj = client.get_object(Bucket=bucket_name, Key=f"{partial_key}02.json")
    obj = json.loads(obj['Body'].read().decode('utf-8'))
    assert obj['meta']['series'] == series
