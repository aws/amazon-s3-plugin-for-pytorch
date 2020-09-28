import numpy as np
import os
import pytest
from awsio.python.lib.io.s3.s3dataset import S3Dataset
from awsio.python.lib.io.s3.s3dataset import (list_files, file_exists,
                                              get_file_size)
import boto3


def test_list_files_prefix():
    from awsio.python.lib.io.s3.s3dataset import S3Dataset
    from awsio.python.lib.io.s3.s3dataset import (list_files, file_exists,
                                                  get_file_size)
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
    os.environ['AWS_REGION'] = 'us-west-2'
    from awsio.python.lib.io.s3.s3dataset import S3Dataset
    from awsio.python.lib.io.s3.s3dataset import (list_files, file_exists,
                                                  get_file_size)
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
    del os.environ['AWS_REGION']


def test_multi_download():
    s3_dataset_path = 's3://ydaiming-test-data2/test_0/test'
    bucket_name = 'ydaiming-test-data2'
    prefix = 'test_0/test'

    if 'S3_DISABLE_MULTI_PART_DOWNLOAD' in os.environ:
        del os.environ['S3_DISABLE_MULTI_PART_DOWNLOAD']
    result1 = list_files(s3_dataset_path)
    s3 = boto3.resource('s3')
    test_bucket = s3.Bucket(bucket_name)
    result2 = []
    for url in test_bucket.objects.filter(Prefix=prefix):
        result2.append('s3://' + url.bucket_name + '/' + url.key)
    assert isinstance(result1, list)
    assert isinstance(result2, list)
    assert result1 == result2


def test_disable_multi_download():
    s3_dataset_path = 's3://ydaiming-test-data2/test_0/test'
    bucket_name = 'ydaiming-test-data2'
    prefix = 'test_0/test'

    os.environ['S3_DISABLE_MULTI_PART_DOWNLOAD'] = "ON"
    result1 = list_files(s3_dataset_path)
    s3 = boto3.resource('s3')
    test_bucket = s3.Bucket(bucket_name)
    result2 = []
    for url in test_bucket.objects.filter(Prefix=prefix):
        result2.append('s3://' + url.bucket_name + '/' + url.key)
    assert isinstance(result1, list)
    assert isinstance(result2, list)
    assert result1 == result2
    del os.environ['S3_DISABLE_MULTI_PART_DOWNLOAD']


@pytest.mark.parametrize("bucket_name, object_name", [("ydaiming-test-data2", "test_0.JPEG"), ("ydaiming-test-data2", "test_new_file.JPEG"), ('ydaiming-test-data2', 'folder_1')])
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


def test_get_file_size():
    bucket_name = 'ydaiming-test-data2'
    object_name = 'test_0.JPEG'
    result1 = get_file_size('s3://' + bucket_name + '/' + object_name)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    result2 = bucket.Object(object_name).content_length
    assert result1 == result2

