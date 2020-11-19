#   Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
  
#   Licensed under the Apache License, Version 2.0 (the "License").
#   You may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
  
#       http://www.apache.org/licenses/LICENSE-2.0
  
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import torch
from torch.utils.data import Dataset, DataLoader
import _pywrap_s3_io
from s3dataset import S3Dataset
from s3dataset import list_files, file_exists
import sys

filenames = list_files('s3://ydaiming-test-data2/test_0.JPEG')
print(filenames)
#f = 's3://ydaiming-test-data2/test_0/test_9970.JPEG'
#f = ['s3://roshanin-test-data/tinyimagenet.tar']
#f = ['s3://roshanin-test-data/tiny-imagenet-200.zip']
dataset = S3Dataset(filenames)
#dataset = S3Dataset('s3://roshanin-test-data/tinyimagenet.tar', compression="tar")
#dataset = S3Dataset('s3://roshanin-test-data/tiny-imagenet-200.zip', compression="zip")
#loader = DataLoader(dataset, batch_size=1)
#print(dataset)
for f1 in dataset:
    print(f1)
#    print(f,content)
print('[Dataset Length]:', len(dataset))
