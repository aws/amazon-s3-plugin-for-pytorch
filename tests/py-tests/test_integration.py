"""
Datasets:
    One Imagenet folder use
    Some Coco images
    Some text files?

Design to catch any bug in code:
    Number of files being read
    Size of files being read
    Content of files? - specified by the size of files read

    Should work by specifying just the prefix folder, url_list, 

Should test both S3Dataset, S3IterableDataset


Logic:
Test1: Establishes all objects are being read from S3
    Read objects using a Dataloader with S3Dataset and S3IterableDataset
    Keep putting object-names with sizes - tuples - in a set.
    Maintain a count of objects read

    Compare the same things after reading from Boto
    Boto works so plugin should be working correctly

Test2: Establish Dataloader works correctly 
    number of batches read in should be correct. 

Test3:
    Add for shuffling, check other functionalities

    

Add later: 
    check to see if shuffling is working.
    set should be same, but lists should not be equal.

    check both  tar and zip files

    remove hardcoding by adding argparser

Discuss:
    Do we add sampler for S3Dataset??
    S3Iterable will not have sampler
"""

from awsio.python.lib.io.s3.s3dataset import S3Dataset, S3IterableDataset
from awsio.python.lib.io.s3.s3dataset import tardata, zipdata

from torch.utils.data.distributed import DistributedSampler

import io
import math
import boto3
from torch.utils.data import DataLoader

def read_using_boto(bucket, prefix):
        fs = io.BytesIO()
        s= boto3.client('s3')
        s.download_fileobj(bucket,
                            prefix,
                            fs)
        file_content = fs.getvalue()

        s3_obj_set = set()

        if prefix[-3:] == "tar":
            tarfile = tardata(file_content)
            for fname, content in tarfile:
                s3_obj_set.add((fname, content))
                
        elif prefix[-3:] == "zip":
            zipfile = zipdata(file_content)
            for fname, content in zipfile:
                s3_obj_set.add((fname, content))

        else:
            s3_obj_set.add((prefix.split("/")[-1]), file_content)
        
        return s3_obj_set



def test_S3IterableDataset(bucket, prefix_list):
    s3_obj_set_boto = set()
    for prefix in prefix_list:
        s3_obj_set_boto.update(read_using_boto(bucket, prefix))
    print ("read using boto")

    s3_obj_set = set()
    batch_size = 32
    url_list = ["s3://" + bucket + "/" + prefix for prefix in prefix_list]
    dataset = S3IterableDataset(url_list)
    expected_batches = math.ceil(len(s3_obj_set_boto)/batch_size)

    for num_workers in [0, 2, 4, 6, 8]:
        dataloader = DataLoader(dataset,
                        batch_size=batch_size, 
                        num_workers=num_workers)
        print ("\nTesting S3Iterable dataset with {} workers".format(num_workers))
        num_batches = 0
        for fname, fobj in dataloader:
           batch_set = set(map(tuple, zip(fname, fobj)))
           s3_obj_set.update(batch_set)
           num_batches += 1
        assert s3_obj_set == s3_obj_set_boto, "Test fails for {} workers".format(num_workers)
        print ("All data correctly loaded for s3Iterable dataset for {} workers".format(num_workers))
        assert expected_batches == num_batches, "Data Incorrectly batched for {} workers".format(num_workers)
        print ("Data correctly batched for s3Iterable dataset for {} workers".format(num_workers))
        

if __name__ == "__main__":
    print ("Let us get started")
    url_list = ["s3://mansmane-dev/imagenet_web_dataset/train/imagenet-train-{}.tar".format(str(0).zfill(6))]
    
    # s3_read_obj = read_using_boto("mansmane-dev", "imagenet_web_dataset/train/imagenet-train-000000.tar")
    # print (len((s3_read_obj)))
    # for name, content in s3_read_obj:
    #     print (name)
    #     print (content)
    #     break

    test_S3IterableDataset("mansmane-dev", ["imagenet_web_dataset/train/imagenet-train-000000.tar"])