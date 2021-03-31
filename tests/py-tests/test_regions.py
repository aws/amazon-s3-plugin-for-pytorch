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
    s3_dataset_path = 's3://pt-s3plugin-test-data-east1/images/n'
    bucket_name = 'pt-s3plugin-test-data-east1'
    prefix = 'images/n'
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
    s3_dataset_path = 's3://pt-s3plugin-test-data-east1/genome-scores.csv'
    dataset = S3Dataset(s3_dataset_path)
    import pandas as pd
    for files in dataset:
        result1 = pd.read_csv(io.BytesIO(files[1]))
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=s3_dataset_path.split('/')[2], Key=s3_dataset_path.split('/')[3])
    result2 = pd.read_csv(io.BytesIO(obj['Body'].read()))
    assert result1.equals(result2)
    del os.environ['AWS_REGION']
