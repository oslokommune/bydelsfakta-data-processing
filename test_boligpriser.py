import unittest
from unittest import TestCase
import os, json
import logging
import boto3
from moto import mock_s3
import boligpriser as bp
import random


# logging.basicConfig(level=logging.DEBUG)

class TestRead_csv(TestCase):

    @mock_s3
    def test_handler(self):
        s3 = boto3.resource('s3', region_name='eu-central-1')
        s3.create_bucket(Bucket=s3_bucket)
        with open('test_data/Boligpriser.csv', 'r') as file:
            bp.pl.write_json_to_s3(s3_bucket, s3_object_key, file.read())

        bp.handler(s3_event, {})

        produced = s3.Bucket(s3_bucket).objects.filter(Prefix='processed/')

        for object in produced:
            print(object.key)
            s3.Bucket(s3_bucket).download_file(object.key, "out/" + object.key.split('/')[-1])

        self.assertEqual(sum(1 for _ in produced), 18*2)



def contains_all_json(json_list):
    districts = True  # TODO: Check that all districts are present
    area = False
    oslo = False

    for item in json_list['data']:
        if 'totalRow' in item:
            oslo = True
        if 'avgRow' in item:
            area = True
        if districts & area & oslo:
            return districts & area & oslo

    logging.debug("district: {file}".format(file=districts))
    logging.debug("area: {file}".format(file=area))
    logging.debug("oslo: {file}".format(file=oslo))
    return False

s3_object_key = 'raw/green/boligpriser/1/2017/Boligpriser.csv'
s3_bucket = 'test-bucket'
s3_event = {
    "Records": [
        {
            "eventVersion": "2.0",
            "eventSource": "aws:s3",
            "awsRegion": "us-west-2",
            "requestParameters": {
                "sourceIPAddress": "ip-address-where-request-came-from"
            },
            "responseElements": {
                "x-amz-request-id": "Amazon S3 generated request ID",
                "x-amz-id-2": "Amazon S3 host that processed the request"
            },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "ID found in the bucket notification configuration",
                "bucket": {
                    "name": s3_bucket,
                    "ownerIdentity": {
                        "principalId": "Amazon-customer-ID-of-the-bucket-owner"
                    },
                    "arn": "bucket-ARN"
                },
                "object": {
                    "key": s3_object_key,
                    "eTag": "object eTag",
                    "versionId": "object version if bucket is versioning-enabled, otherwise null",
                    "sequencer": "a string representation of a hexadecimal value used to determine event sequence"
                }
            }
        }
    ]
}
