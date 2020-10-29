import os
import sys
import torch

import numpy as np
from torch.utils.data import IterableDataset
from mmcv.parallel import collate


# from .builder import DATASETS
from s3dataset import S3IterableDataset
from torch.utils.data import DataLoader
from functools import partial

# from .pipelines import Compose

"""
!!! Remove the below comment later on
module has to be registered
"""
#@DATASETS.register_module()
class ImageNetS3(IterableDataset):
    def __init__(self, pipeline):
        self.url_list = ["s3://mansmane-dev/imagenet_web_dataset/train/imagenet-train-{}.tar".format(str(0).zfill(6))]

    def my_generator(self):
        try:
            while True:
                label_fname, label_fobj = next(self.s3_iter_dataset_iterator)
                image_fname, image_fobj = next(self.s3_iter_dataset_iterator)

                print (label_fname, image_fname)

                label = int(label_fobj)
                # print(label, type(label))
                # print (label_fobj, type(label_fobj))
                yield torch.Tensor(1)
        except StopIteration:
            raise StopIteration

    def __iter__(self):
        self.s3_iter_dataset = S3IterableDataset(self.url_list, shuffle_urls=True)
        self.s3_iter_dataset_iterator = iter(self.s3_iter_dataset)
        return self.my_generator()


if __name__ == "__main__":
    dataset = ImageNetS3("random")
    dataloader = DataLoader(dataset,
        batch_size=32 ,
        sampler=None,
        num_workers=2)

    #Loads 96 lines at a time. ? Any reason? (3 batches)
    for i, data_batch in enumerate(dataloader):
        print (i)
        print ("works")