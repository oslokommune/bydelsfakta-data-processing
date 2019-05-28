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
    output_list = [{"bydel_id": "01"}, {"bydel_id": "02"}, {"bydel_id": "03"}]
    aws.write_to_intermediate(partial_key, output_list)
    obj = client.get_object(Bucket=bucket_name, Key=f"{partial_key}02.json")
    obj = json.loads(obj["Body"].read().decode("utf-8"))
    assert obj == output_list[1]
