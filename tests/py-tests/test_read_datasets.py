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
import io
import pytest
from awsio.python.lib.io.s3.s3dataset import S3Dataset, S3IterableDataset
from awsio.python.lib.io.s3.s3dataset import list_files, file_exists
import boto3


def get_tar(s3_dataset_path):
    s3 = boto3.client('s3')
    s3.download_file(
        s3_dataset_path.split('/')[2],
        s3_dataset_path.split('/')[3], '/tmp/input_file.tar')
    import tarfile
    stream = tarfile.open('/tmp/input_file.tar')
    filenames_boto3 = []
    for tarinfo in stream:
        fname = tarinfo.name
        stream.extractfile(tarinfo).read()
        filenames_boto3.append(fname)
    return filenames_boto3


def test_tar_file_s3dataset():
    s3_dataset_path = 's3://ydaiming-test-data2/tinyimagenet.tar'
    dataset = S3Dataset(s3_dataset_path)
    fileobj = io.BytesIO(dataset[0][1])
    import tarfile
    with tarfile.open(fileobj=fileobj, mode="r|*") as tar:
        result1 = len(tar.getmembers())
    result2 = get_tar(s3_dataset_path)
    assert result1 == len(result2)


def test_tar_file_s3iterabledataset():
    s3_dataset_path = 's3://ydaiming-test-data2/tinyimagenet.tar'
    dataset = S3IterableDataset(s3_dataset_path)
    list_of_files = []
    for files in dataset:
        list_of_files.append(files[0][0])
    result1 = len(list_of_files)
    result2 = get_tar(s3_dataset_path)
    assert result1 == len(result2)


def get_zip(s3_dataset_path):
    s3 = boto3.client('s3')
    s3.download_file(
        s3_dataset_path.split('/')[2],
        s3_dataset_path.split('/')[3], '/tmp/input_file.zip')
    import zipfile
    filenames_boto3 = []
    with zipfile.ZipFile('/tmp/input_file.zip', 'r') as zfile:
        for file_ in zfile.namelist():
            zfile.read(file_)
            filenames_boto3.append(file_)
    return filenames_boto3


def test_zip_file_s3dataset():
    s3_dataset_path = 's3://ydaiming-test-data2/tiny-imagenet-200.zip'
    dataset = S3Dataset(s3_dataset_path)
    fileobj = io.BytesIO(dataset[0][1])
    import zipfile
    with zipfile.ZipFile(fileobj, 'r') as zfile:
        result1 = len(zfile.namelist())
    result2 = get_zip(s3_dataset_path)
    assert result1 == len(result2)


def test_zip_file_s3iterabledataset():
    s3_dataset_path = 's3://ydaiming-test-data2/tiny-imagenet-200.zip'
    dataset = S3IterableDataset(s3_dataset_path)
    list_of_files = []
    for files in dataset:
        list_of_files.append(files[0][0])
    result1 = len(list_of_files)
    result2 = get_zip(s3_dataset_path)
    assert result1 == len(result2)


def test_csv_file_s3dataset():
    os.environ['AWS_REGION'] = 'us-west-2'
    s3_dataset_path = 's3://ydaiming-test-data2/genome-scores.csv'
    dataset = S3Dataset(s3_dataset_path)
    import pandas as pd
    result1 = pd.read_csv(io.BytesIO(dataset[0][1]))
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=s3_dataset_path.split('/')[2],
                        Key=s3_dataset_path.split('/')[3])
    result2 = pd.read_csv(io.BytesIO(obj['Body'].read()))
    assert result1.equals(result2)
    del os.environ['AWS_REGION']


def test_csv_file_s3iterabledataset():
    os.environ['AWS_REGION'] = 'us-west-2'
    s3_dataset_path = 's3://ydaiming-test-data2/genome-scores.csv'
    dataset = S3IterableDataset(s3_dataset_path)
    import pandas as pd
    for files in dataset:
        result1 = pd.read_csv(io.BytesIO(files[1]))
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=s3_dataset_path.split('/')[2], Key=s3_dataset_path.split('/')[3])
    result2 = pd.read_csv(io.BytesIO(obj['Body'].read()))
    assert result1.equals(result2)
    del os.environ['AWS_REGION']
