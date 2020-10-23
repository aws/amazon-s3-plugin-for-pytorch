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
    url_list = ["s3://mansmane-dev/imagenet_web_dataset/train/imagenet-train-{}.tar".format(str(filenum).zfill(6)) for filenum in range(299)]

    imagenet_s3 = S3IterableDataset(url_list)

    # imagenet_loader = DataLoader(imagenet_s3,  collate_fn=collate_fn, batch_size = 2)
    imagenet_loader = DataLoader(imagenet_s3, batch_size = 64)

    i = 0

    """
    iterator returns (batch is) a list of tuples
    The list is the list of tuples
    First tuple contains the filenames
    Second tuple contains the binary blob of the file - fileobjs

    filobjs contains the data to train the model
    fileobjs[n] is the label in binary format and fileobjs[n+1] is the image in binary format
    for n = 0, 2, 4, ....

    label has to be converted to integer format. Check how image is consumed 
    mmclassification dataloader returns a dictionary of filename and integer numpy array
    print and see how that works
    """
    for batch in imagenet_loader:
        print (type(batch))
        filenames, fileobjs = batch
        print (filenames)
        print (fileobjs[0], fileobjs[2], type(fileobjs[1]))
        # if i == 0:
        #     print (fileobjs)
        # print (fileobj[0])
        if i == 1:
            break
        i += 1