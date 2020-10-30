import torch
from s3dataset import S3IterableDataset
from torch.utils.data import DataLoader
import sys

def collate_fn(batch):
        print (type(batch))
        filenames, fileobjs = batch
        print (filenames)
        print (fileobjs[0])

        return batch

if __name__ == "__main__":
    url_list = ["s3://mansmane-dev/imagenet_web_dataset/train/imagenet-train-{}.tar".format(str(0).zfill(6))]

    imagenet_s3 = iter(S3IterableDataset(url_list))

    # imagenet_loader = DataLoader(imagenet_s3,  collate_fn=collate_fn, batch_size = 2)
    # imagenet_loader = DataLoader(imagenet_s3, batch_size = 1)

    # i = 0
    # for batch in imagenet_loader:
    #     # print (type(batch))
    #     filenames, fileobjs = batch
    #     print (filenames)
    #     # print (fileobjs[0], fileobjs[2], type(fileobjs[1]))
    #     # if i == 0:
    #     #     print (fileobjs)
    #     # print (fileobj[0])
    #     if i == 1:
    #         break
    #     i += 1

    while True:
        try:
            filenames, fileobjs = next(imagenet_s3)
            print (filenames)
        except StopIteration:
            print("Wow this works")
            break