import numpy as np
import os
import pytest
from awsio.python.lib.io.s3.s3dataset import S3Dataset
from awsio.python.lib.io.s3.s3dataset import list_files
import boto3

def test_list_files_prefix():
    s3_dataset_path = 's3://ydaiming-test-data2/test_0/test'
    result1 = list_files(s3_dataset_path)
    s3 = boto3.resource('s3')
    test_bucket = s3.Bucket('ydaiming-test-data2')
    result2 = []
    for url in test_bucket.objects.filter(Prefix='test_0/test'):
        result2.append('s3://'+url.bucket_name+'/'+url.key)
    assert isinstance(result1, list)
    assert isinstance(result2, list)
    assert result1 == result2

def test_list_files_bucket():
    s3_dataset_path = 's3://ydaiming-test-data2'
    result1 = list_files(s3_dataset_path)
    s3 = boto3.resource('s3')
    test_bucket = s3.Bucket('ydaiming-test-data2')
    result2 = []
    for url in test_bucket.objects.filter(Delimiter='/'):
        result2.append('s3://'+url.bucket_name+'/'+url.key)
    assert isinstance(result1, list)
    assert isinstance(result2, list)
    assert result1 == result2


test_list_files_prefix()
test_list_files_bucket()
