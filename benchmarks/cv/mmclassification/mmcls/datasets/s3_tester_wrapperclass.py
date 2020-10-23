import os

import numpy as np
from torch.utils.data import IterableDataset

# from .builder import DATASETS
from s3dataset import S3IterableDataset

# from .pipelines import Compose

"""
!!! Remove the below comment later on
module has to be registered
"""
#@DATASETS.register_module()
class ImageNetS3(IterableDataset):
    """
    Class is a wrapper over S3IterableDataset and implements functionality of basedataset
    This ensures that it can directly be plugged and used with mmclassification

    S3IterableDataset already gives the image in binary format. 
    The binary image blob is made into correct format - so that it can fit through the remaining
    data pipeline. 
    The rest of the transformations are then applied to the blob and the transformed sample is returned

    Currently generating URL lists in the constructor itself. and shuffle=True
    Can later pass as an argument
    """
    def __init__(self, pipeline):
        #url_list = ["s3://mansmane-dev/imagenet_web_dataset/train/imagenet-train-{}.tar".format(str(filenum).zfill(6)) for filenum in range(299)]
        url_list = ["s3://mansmane-dev/imagenet_web_dataset/train/imagenet-train-{}.tar".format(str(0).zfill(6))]
        self.s3_iter_dataset_iterator = iter(S3IterableDataset(url_list, shuffle_urls=True))
        #self.pipeline = Compose(pipeline)

    def __iter__(self):
        return self
    
    """
    Very Strong Assumption here:
        The dataset is 'tar' in a way that labelfile is first and corresponding image
        is second. 
        Current - webdataset compatible tar file - has this sturcture
    """
    def __next__(self):
        try:
            label_fname, label_fobj = next(self.s3_iter_dataset_iterator)
            image_fname, image_fobj = next(self.s3_iter_dataset_iterator)

            print (label_fname, image_fname)

            label = int(label_fobj)
            print(label, type(label))
            print (label_fobj, type(label_fobj))
        except StopIteration:
            raise StopIteration

if __name__ == "__main__":
    for i in ImageNetS3("random"):
        print ("works")