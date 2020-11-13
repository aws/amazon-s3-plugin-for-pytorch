import numpy as np
import os
import io
import pytest
from awsio.python.lib.io.s3.s3dataset import S3Dataset
from awsio.python.lib.io.s3.s3dataset import (list_files, file_exists,
                                              get_file_size)
import boto3

def test_regions():
    os.environ['AWS_REGION'] = 'us-east-1'
    s3_dataset_path = 's3://roshanin-dev/test/n'
    bucket_name = 'roshanin-dev'
    prefix = 'test/n'
    result1 = list_files(s3_dataset_path)
    s3 = boto3.resource('s3')
    test_bucket = s3.Bucket(bucket_name)
    result2 = []
    for url in test_bucket.objects.filter(Prefix=prefix):
        result2.append('s3://' + url.bucket_name + '/' + url.key)
    assert isinstance(result1, list)
    assert isinstance(result2, list)
    assert result1 == result2
    del os.environ['AWS_REGION']


def test_csv_file():
    os.environ['AWS_REGION'] = 'us-east-1'
    s3_dataset_path = 's3://roshanin-dev/genome-scores.csv'
    dataset = S3Dataset(s3_dataset_path)
    import pandas as pd
    for files in dataset:
        result1 = pd.read_csv(io.BytesIO(files[1]))
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=s3_dataset_path.split('/')[2], Key=s3_dataset_path.split('/')[3])
    result2 = pd.read_csv(io.BytesIO(obj['Body'].read()))
    assert result1.equals(result2)
    del os.environ['AWS_REGION']
