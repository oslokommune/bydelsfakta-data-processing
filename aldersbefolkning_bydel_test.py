#!/usr/bin/env python
# -*- coding: utf-8 -*-
import unittest

import moto, boto3
import pandas as pd
from moto import mock_s3

import aldersbefolkning_bydel

s3Event = {
    "Records": [
        {
            "eventVersion": "2.0",
            "eventSource": "aws:s3",
            "awsRegion": "eu-west-1",
            "eventTime": "1970-01-01T00:00:00.000Z",
            "eventName": "ObjectCreated:Put",
            "userIdentity": {
                "principalId": "EXAMPLE"
            },
            "requestParameters": {
                "sourceIPAddress": "127.0.0.1"
            },
            "responseElements": {
                "x-amz-request-id": "EXAMPLE123456789",
                "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
            },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "testConfigRule",
                "bucket": {
                    "name": "ok-origo-dataplatform-dev",
                    "ownerIdentity": {
                        "principalId": "EXAMPLE"
                    },
                    "arn": "arn:aws:s3:::/ok-origo-dataplatform-dev"
                },
                "object": {
                    "key": "raw/green/befolkningen_etter_bydel_og_aldersgrupper/1/1547742879/Befolkningen_etter_bydel_delbydel_kjonn_og_1-aars_aldersgrupper.csv",
                    "size": 1024,
                    "eTag": "0123456789abcdef0123456789abcdef",
                    "sequencer": "0A1B2C3D4E5F678901"
                }
            }
        }
    ]
}


class Tester(unittest.TestCase):
    bucket = "dataplatform"


    ## TODO: create usable test data
    @mock_s3
    def test(self):
        s3 = boto3.resource('s3', region_name='eu-central-1')
        s3.create_bucket(Bucket=self.bucket)
        test_file = "test_files/Befolkningen_etter_bydel_delbydel_kjonn_og_1-aars_aldersgrupper.csv"
        csv_path = "raw/green/befolkningen_etter_bydel_og_aldersgrupper/1/1547742879/Befolkningen_etter_bydel_delbydel_kjonn_og_1-aars_aldersgrupper.csv"
        with open(test_file, 'rb') as f:
            s3.Object(self.bucket, csv_path).put(Body=f)

        csv_source = pd.read_csv('s3://{bucket}/{key}'.format(bucket=self.bucket, key=csv_path),
                                 sep=";",
                                 encoding='utf8')
        initialized = aldersbefolkning_bydel.init_dataframe(csv_source)

        transformed = aldersbefolkning_bydel.transform(initialized)

        listed = [(aldersbefolkning_bydel.create_specific_bydel(district, transformed)) for district in aldersbefolkning_bydel.districts]



if __name__ == '__main__':
    unittest.main()
