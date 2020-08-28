import torch
from torch.utils.data import Dataset
import _pywrap_s3_io

class S3Dataset(Dataset):
    localfile = 'tempfile.txt'
    def __init__(self, bucket, filename):
        p = _pywrap_s3_io.S3Init()
        filename = "s3://roshanin-test-data/tiny-imagenet-200.zip"
#        filename = "s3://roshanin-test-data/train-images-idx3-ubyte.gz"
#        filename = "s3://roshanin-dev/n07871810_6.JPEG"
#        filename = "s3://ydaiming-test-data/genome-scores.csv"
        self.data = p.s3_read(filename, False)
        #self.s3.download_file(bucket, fmilename, self.localfile)

        # open file and set it to data
       # self.data = open(self.localfile, "r").read()

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        return self.data[idx]

dataset = S3Dataset('hpptel-dev', 'tiny-imagenet-200.zip')
print('Sample snippet: ', dataset[442:475])
print('Size: ',  len(dataset))
