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
from awsio.python.lib.io.s3.s3dataset import S3IterableDataset, ShuffleDataset
import boto3


def test_file_path():
    """
    Test S3IterableDataset for existing and nonexistent path
    """
    # existing path
    s3_path = 's3://ydaiming-test-data2/test_0/test'
    s3_dataset = S3IterableDataset(s3_path)
    assert s3_dataset

    # non-existent path
    s3_path_none = 's3://ydaiming-test-data2/non_existent_path/test'
    with pytest.raises(AssertionError) as excinfo:   
        s3_dataset = S3IterableDataset(s3_path_none)
    assert 'does not contain any objects' in str(excinfo.value)


def test_urls_list():
    """
    Test whether urls_list input for S3IterableDataset works properly
    """
    os.environ['AWS_REGION'] = 'us-west-2'
    # provide url prefix (path within bucket)
    prefix_to_directory = 'test_0/test'
    prefix_to_file = 'test_1.JPEG'
    prefix_list=[prefix_to_directory, prefix_to_file]

    # set up boto3
    s3 = boto3.resource('s3')
    bucket_name = 'ydaiming-test-data2'
    test_bucket = s3.Bucket(bucket_name)

    # try individual valid urls and collect url_list and all_boto3_files to test url list input
    urls_list = list()
    all_boto3_files = list()
    for prefix in prefix_list:
        # collect list of all file names using S3IterableDataset
        url = os.path.join('s3://', bucket_name, prefix)
        urls_list.append(url)
        s3_dataset = S3IterableDataset(url)
        s3_files = [item[0] for item in s3_dataset]

        # collect list of all file names using boto3
        boto3_files = [os.path.join('s3://', url.bucket_name, url.key) \
            for url in test_bucket.objects.filter(Prefix=prefix)]
        all_boto3_files.extend(boto3_files)

        assert s3_files == boto3_files

    # test list of two valid urls as input
    s3_dataset = S3IterableDataset(urls_list)
    s3_files = [item[0] for item in s3_dataset]

    assert s3_files == all_boto3_files

    # add an non-existent url to list of urls
    url_to_non_existent = 's3://ydaiming-test-data2/non_existent_directory'
    urls_list.append(url_to_non_existent)
    with pytest.raises(AssertionError) as excinfo:   
        s3_dataset = S3IterableDataset(urls_list)
    assert 'does not contain any objects' in str(excinfo.value)    

    del os.environ['AWS_REGION']


def test_shuffle_true():
    """
    Tests shuffle_urls parameter, len and  set_epoch functions
    """
    os.environ['AWS_REGION'] = 'us-west-2'

    # create two datasets, one shuffled with self.epoch
    s3_dataset_path = 's3://ydaiming-test-data2/test_0/test'
    s3_dataset0 = S3IterableDataset(s3_dataset_path)
    s3_dataset1 = S3IterableDataset(s3_dataset_path, shuffle_urls=True)
    s3_dataset1.set_epoch(5)

    # len is defined as the length of the urls_list created by the path
    assert len(s3_dataset0) == len(s3_dataset1)

    # check to make sure shuffling works
    filenames0 = [item[0] for item in s3_dataset0]
    filenames1 = [item[0] for item in s3_dataset1]

    assert len(filenames0) == len(filenames1)
    assert filenames0 != filenames1
    del os.environ['AWS_REGION']


def test_multi_download():
    s3_dataset_path = 's3://ydaiming-test-data2/genome-scores.csv'

    if 'S3_DISABLE_MULTI_PART_DOWNLOAD' in os.environ:
        del os.environ['S3_DISABLE_MULTI_PART_DOWNLOAD']
    os.environ['AWS_REGION'] = 'us-west-2'

    dataset = S3IterableDataset(s3_dataset_path)
    import pandas as pd
    for files in dataset:
        result1 = pd.read_csv(io.BytesIO(files[1]))
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=s3_dataset_path.split('/')[2],
                        Key=s3_dataset_path.split('/')[3])
    result2 = pd.read_csv(io.BytesIO(obj['Body'].read()))
    assert result1.equals(result2)


def test_disable_multi_download():
    s3_dataset_path = 's3://ydaiming-test-data2/genome-scores.csv'
    os.environ['S3_DISABLE_MULTI_PART_DOWNLOAD'] = "ON"
    os.environ['AWS_REGION'] = 'us-west-2'
    dataset = S3IterableDataset(s3_dataset_path)
    import pandas as pd
    for files in dataset:
        result1 = pd.read_csv(io.BytesIO(files[1]))
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=s3_dataset_path.split('/')[2],
                        Key=s3_dataset_path.split('/')[3])
    result2 = pd.read_csv(io.BytesIO(obj['Body'].read()))
    assert result1.equals(result2)
    del os.environ['S3_DISABLE_MULTI_PART_DOWNLOAD'], os.environ['AWS_REGION']


def test_shuffle_dataset():

    dataset = [i for i in range(10)]

    # buffer_size 1 should yield the dataset without shuffling
    shuffle_dataset = ShuffleDataset(dataset=dataset, buffer_size=1)
    shuffle_content = [item for item in shuffle_dataset]
    assert dataset == shuffle_content

    # buffer_size smaller than dataset size
    shuffle_dataset = ShuffleDataset(dataset=dataset, buffer_size=2)
    assert set(dataset) == set(shuffle_content)
    assert len(dataset) == len(shuffle_content)

    # buffer_size greater than dataset size
    shuffle_dataset = ShuffleDataset(dataset=dataset, buffer_size=15)
    assert set(dataset) == set(shuffle_content)
    assert len(dataset) == len(shuffle_content)
