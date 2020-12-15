#   Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#   Licensed under the Apache License, Version 2.0 (the "License").
#   You may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import os
import pytest
from awsio.python.lib.io.s3.s3dataset import S3Dataset
import boto3

bucket = "ydaiming-test-data2"
os.environ['AWS_REGION'] = 'us-west-2'


def test_file_path():
    """
    Test S3Dataset for existing and nonexistent path
    """
    # existing path
    s3_path = 's3://' + bucket + '/test_0/test'
    s3_dataset = S3Dataset(s3_path)
    assert s3_dataset

    # non-existent path
    s3_path_none = 's3://' + bucket + '/non_existent_path/test'
    with pytest.raises(AssertionError) as excinfo:   
        s3_dataset = S3Dataset(s3_path_none)
    assert 'does not contain any objects' in str(excinfo.value)


def test_urls_list():
    """
    Test whether urls_list input for S3Dataset works properly
    """
    # provide url prefix (path within bucket)
    prefix_to_directory = 'test_0/test'
    prefix_to_file = 'test_1.JPEG'
    prefix_list=[prefix_to_directory, prefix_to_file]

    # set up boto3
    s3 = boto3.resource('s3')
    bucket_name = bucket
    test_bucket = s3.Bucket(bucket_name)

    # try individual valid urls and collect url_list and all_boto3_files to test url list input
    urls_list = list()
    all_boto3_files = list()
    for prefix in prefix_list:
        # collect list of all file names using S3Dataset
        url = os.path.join('s3://', bucket_name, prefix)
        urls_list.append(url)
        s3_dataset = S3Dataset(url)
        s3_files = [item[0] for item in s3_dataset]

        # collect list of all file names using boto3
        boto3_files = [os.path.join('s3://', url.bucket_name, url.key) \
            for url in test_bucket.objects.filter(Prefix=prefix)]
        all_boto3_files.extend(boto3_files)

        assert s3_files == boto3_files

    # test list of two valid urls as input
    s3_dataset = S3Dataset(urls_list)
    s3_files = [item[0] for item in s3_dataset]

    assert s3_files == all_boto3_files

    # add an non-existent url to list of urls
    url_to_non_existent = 's3://' + bucket + '/non_existent_directory'
    urls_list.append(url_to_non_existent)
    with pytest.raises(AssertionError) as excinfo:   
        s3_dataset = S3Dataset(urls_list)
    assert 'does not contain any objects' in str(excinfo.value)    


def test_multi_download():
    """
    Test whether S3Dataset with multiple downloads in one url works properly
    """
    s3_dataset_path = 's3://' + bucket + '/test_0/test'
    bucket_name = bucket
    prefix = 'test_0/test'

    if 'S3_DISABLE_MULTI_PART_DOWNLOAD' in os.environ:
        del os.environ['S3_DISABLE_MULTI_PART_DOWNLOAD']

    dataset = S3Dataset(s3_dataset_path)
    # collect filename from each item in dataset
    result1 = [item[0] for item in dataset]
    s3 = boto3.resource('s3')
    test_bucket = s3.Bucket(bucket_name)
    result2 = []
    for url in test_bucket.objects.filter(Prefix=prefix):
        result2.append('s3://' + url.bucket_name + '/' + url.key)
    assert isinstance(result1, list)
    assert isinstance(result2, list)
    assert result1 == result2


def test_disable_multi_download():
    s3_dataset_path = 's3://' + bucket + '/test_0/test'
    os.environ['S3_DISABLE_MULTI_PART_DOWNLOAD'] = "ON"
    dataset = S3Dataset(s3_dataset_path)
    result1 = [item[0] for item in dataset]

    # boto3
    bucket_name = bucket
    prefix = 'test_0/test'
    s3 = boto3.resource('s3')
    test_bucket = s3.Bucket(bucket_name)
    result2 = ['s3://' + url.bucket_name + '/' + url.key \
        for url in test_bucket.objects.filter(Prefix=prefix)]

    assert isinstance(result1, list)
    assert isinstance(result2, list)
    assert result1 == result2
    del os.environ['S3_DISABLE_MULTI_PART_DOWNLOAD']
