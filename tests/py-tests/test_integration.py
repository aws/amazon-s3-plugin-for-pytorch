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
"""

from awsio.python.lib.io.s3.s3dataset import S3Dataset, S3IterableDataset
from awsio.python.lib.io.s3.s3dataset import tardata, zipdata

import io
import boto3

def read_using_boto(bucket, filename):
        fs = io.BytesIO()
        s= boto3.client('s3')
        s.download_fileobj(bucket,
                            filename,
                            fs)
        file_content = fs.getvalue()

        s3_obj_set = set()

        if filename[-3:] == "tar":
            tarfile = tardata(file_content)
            for fname, content in tarfile:
                s3_obj_set.add((fname, content))
                
        elif filename[-3:] == "zip":
            zipfile = zipdata(file_content)
            for fname, content in zipfile:
                s3_obj_set.add((fname, content))

        else:
            s3_obj_set.add((filename.split("/")[-1]), file_content)
        
        return s3_obj_set




if __name__ == "__main__":
    print ("Let us get started")
    url_list = ["s3://mansmane-dev/imagenet_web_dataset/train/imagenet-train-{}.tar".format(str(0).zfill(6))]
    
    s3_read_obj = read_using_boto("mansmane-dev", "imagenet_web_dataset/train/imagenet-train-000000.tar")
    print (len((s3_read_obj)))
    for name, content in s3_read_obj:
        print (name)
        print (content)
        break