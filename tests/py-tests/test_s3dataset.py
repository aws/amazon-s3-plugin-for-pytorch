import numpy as np
import os
import io
import pytest
from awsio.python.lib.io.s3.s3dataset import S3Dataset
from awsio.python.lib.io.s3.s3dataset import (list_files, file_exists,
                                              get_file_size)
import boto3


def test_wrong_filenames():
    filenames = ['', 'shor', 'not_start_s3', 's3://', 's3:///no_bucket']
    functions = [list_files, file_exists, get_file_size]
    exception = False
    for function in functions:
        for filename in filenames:
            try:
                function(filename)
            except ValueError:
                exception = True
            assert exception
            exception = False


def test_list_files_prefix():
    # default region is us-west-2
    s3_dataset_path = 's3://ydaiming-test-data2/test_0/test'
    result1 = list_files(s3_dataset_path)
    s3 = boto3.resource('s3')
    test_bucket = s3.Bucket('ydaiming-test-data2')
    result2 = []
    for url in test_bucket.objects.filter(Prefix='test_0/test'):
        result2.append('s3://' + url.bucket_name + '/' + url.key)
    assert isinstance(result1, list)
    assert isinstance(result2, list)
    assert result1 == result2


def test_list_files_bucket():
    # default region is us-west-2
    s3_dataset_path = 's3://ydaiming-test-data2'
    result1 = list_files(s3_dataset_path)
    s3 = boto3.resource('s3')
    test_bucket = s3.Bucket('ydaiming-test-data2')
    result2 = []
    for url in test_bucket.objects.filter(Delimiter='/'):
        result2.append('s3://' + url.bucket_name + '/' + url.key)
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
        result2.append('s3://' + url.bucket_name + '/' + url.key)
    assert isinstance(result1, list)
    assert isinstance(result2, list)
    assert result1 == result2
    del os.environ['AWS_REGION']


def test_multi_download():
    s3_dataset_path = 's3://roshanin-dev/genome-scores.csv'
    if 'S3_DISABLE_MULTI_PART_DOWNLOAD' in os.environ:
        del os.environ['S3_DISABLE_MULTI_PART_DOWNLOAD']
    os.environ['AWS_REGION'] = 'us-east-1'
    dataset = S3Dataset(s3_dataset_path)
    import pandas as pd
    result1 = pd.read_csv(io.BytesIO(dataset[0][1]))
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=s3_dataset_path.split('/')[2],
                        Key=s3_dataset_path.split('/')[3])
    result2 = pd.read_csv(io.BytesIO(obj['Body'].read()))
    assert result1.equals(result2)


def test_disable_multi_download():
    s3_dataset_path = 's3://roshanin-dev/genome-scores.csv'
    os.environ['S3_DISABLE_MULTI_PART_DOWNLOAD'] = "ON"
    os.environ['AWS_REGION'] = 'us-east-1'
    dataset = S3Dataset(s3_dataset_path)
    import pandas as pd
    result1 = pd.read_csv(io.BytesIO(dataset[0][1]))
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=s3_dataset_path.split('/')[2],
                        Key=s3_dataset_path.split('/')[3])
    result2 = pd.read_csv(io.BytesIO(obj['Body'].read()))
    assert result1.equals(result2)
    del os.environ['S3_DISABLE_MULTI_PART_DOWNLOAD'], os.environ['AWS_REGION']


def test_file_exists(bucket_name, object_name):
    result1 = file_exists('s3://' + bucket_name + '/' + object_name)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    objs = list(bucket.objects.filter(Prefix=object_name))
    if objs and any([w.key == object_name for w in objs]):
        result2 = True
    else:
        result2 = False
    assert result1 == result2


def test_get_file_size(bucket_name, object_name):
    try:
        result1 = get_file_size('s3://' + bucket_name + '/' + object_name)
    except ValueError:
        result1 = False
    try:
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_name)
        result2 = bucket.Object(object_name).content_length
    except Exception:
        result2 = False
    assert result1 == result2


test_wrong_filenames()
test_list_files_prefix()
test_list_files_bucket()
test_regions('us-east-1', 's3://roshanin-dev/test/n', 'roshanin-dev', 'test/n')
test_file_exists('ydaiming-test-data2', 'test_0.JPEG')
test_file_exists('ydaiming-test-data2', 'test_new_file.JPEG')
test_file_exists('ydaiming-test-data2', 'folder_1')
test_get_file_size('ydaiming-test-data2', 'test_0.JPEG')
test_get_file_size('ydaiming-test-data2', 'test_0')
test_multi_download()
test_disable_multi_download()
