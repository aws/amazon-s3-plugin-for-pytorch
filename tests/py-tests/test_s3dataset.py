import numpy as np
import os
import pytest
from awsio.python.lib.io.s3.s3dataset import S3Dataset
from awsio.python.lib.io.s3.s3dataset import list_files, file_exists
import boto3

def test_list_files_prefix():
    #default region is us-west-2
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
    #default region is us-west-2
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


def test_regions(region, s3_dataset_path, bucket_name, prefix):
    os.environ['AWS_REGION'] = region
    result1 = list_files(s3_dataset_path)
    s3 = boto3.resource('s3')
    test_bucket = s3.Bucket(bucket_name)
    result2 = []
    for url in test_bucket.objects.filter(Prefix=prefix):
        result2.append('s3://'+url.bucket_name+'/'+url.key)
    assert isinstance(result1, list)
    assert isinstance(result2, list)
    assert result1 == result2


def test_file_exists(bucket_name, object_name):
    result1 = file_exists(bucket_name, object_name)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    objs = list(bucket.objects.filter(Prefix=object_name))
    if not objs and any([w.Key == object_name for w in objs]):
        result2= True
    else:
        result2 = False

    assert result1 == result2


test_list_files_prefix()
test_list_files_bucket()
test_regions('us-east-1', 's3://roshanin-dev/test/n', 'roshanin-dev', 'test/n')
test_file_exists('ydaiming-test-data2', 'test_0.JPEG')
test_file_exists('ydaiming-test-data2', 'test_new_file.JPEG')
