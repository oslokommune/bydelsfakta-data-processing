import pytest
import json

import common.aws as aws


def test_write_files(s3, bucket_name):
    partial_key = "intermediate/some/value/here/"
    output_list = [{"district": "01"}, {"district": "02"}, {"district": "03"}]
    aws.write_to_intermediate(partial_key, output_list)
    obj = s3.get_object(Bucket=bucket_name, Key=f"{partial_key}02.json")
    obj = json.loads(obj["Body"].read().decode("utf-8"))
    assert obj == output_list[1]


class TestFindS3Key:
    prefix = "processed/green/foo/bar"

    def test_one_file(self, s3, bucket_name):
        key = f"{self.prefix}/stuff.csv"
        s3.put_object(Bucket=bucket_name, Key=key, Body="mm")

        assert aws.find_s3_key(self.prefix) == key

    def test_multiple_files(self, s3, bucket_name):
        key1 = f"{self.prefix}/b_middle.csv"
        key2 = f"{self.prefix}/a_first.csv"
        key3 = f"{self.prefix}/c_last.csv"

        s3.put_object(Bucket=bucket_name, Key=key1, Body="mm")
        s3.put_object(Bucket=bucket_name, Key=key3, Body="mm")
        s3.put_object(Bucket=bucket_name, Key=key2, Body="mm")

        with pytest.raises(aws.MultipleFilesInPrefixError):
            aws.find_s3_key(self.prefix)

    def test_empty_dir(self, s3):
        with pytest.raises(aws.EmptyPrefixError):
            aws.find_s3_key(self.prefix)
