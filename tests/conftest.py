import os
import boto3
import pytest
from moto import mock_s3

from common.aws import s3_bucket


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture
def bucket_name():
    return s3_bucket


@pytest.fixture(scope="function")
def s3(aws_credentials, bucket_name):
    with mock_s3():
        s3 = boto3.client("s3", region_name="eu-west-1")
        s3.create_bucket(Bucket=bucket_name)
        yield s3
