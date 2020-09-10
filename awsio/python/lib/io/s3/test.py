import torch
from torch.utils.data import Dataset, DataLoader
import _pywrap_s3_io
from s3dataset import S3Dataset
from s3dataset import list_files
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
