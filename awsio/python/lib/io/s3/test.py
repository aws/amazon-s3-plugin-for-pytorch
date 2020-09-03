import torch
from torch.utils.data import Dataset
import _pywrap_s3_io
from s3dataset import S3Dataset

dataset = S3Dataset('s3://roshanin-test-data/tinyimagenet.tar', compression="tar")
#dataset = S3Dataset('s3://roshanin-test-data/tiny-imagenet-200.zip', compression="zip")
#print('Sample snippet: ', dataset[442:475])
for f, content  in dataset:
        print(f, content)
       
