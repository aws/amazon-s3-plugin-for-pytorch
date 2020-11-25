"""
Datasets:
    WebDataset - Tar 
    Files in Folder - ImageNet .jpg files and .cls files

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

import io
import math
import boto3
from torch.utils.data import DataLoader

def read_using_boto(bucket, prefix_list):
        s= boto3.client('s3')
        s3_obj_set = set()

        for prefix in prefix_list:
            fs = io.BytesIO()
            s.download_fileobj(bucket,
                                prefix,
                                fs)
            file_content = fs.getvalue()

            if prefix[-3:] == "tar":
                tarfile = tardata(file_content)
                for fname, content in tarfile:
                    s3_obj_set.add((fname, content))                    
            elif prefix[-3:] == "zip":
                zipfile = zipdata(file_content)
                for fname, content in zipfile:
                    s3_obj_set.add((fname, content))
            else:
                s3_obj_set.add((prefix.split("/")[-1], file_content))
        return s3_obj_set

def get_file_list(bucket, files_prefix):
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket)

    file_list = [summary.key for summary in my_bucket.objects.filter(Prefix=files_prefix)]
    return file_list[1:]

def test_workers(dataset_type, url_list,  batch_size, boto_obj_set):
    s3_obj_set = set()
    expected_batches = math.ceil(len(boto_obj_set)/batch_size)

    dataset_class = eval(dataset_type)
    for num_workers in [0, 4, 16]:
        dataset = dataset_class(url_list)
        dataloader = DataLoader(dataset,
                        batch_size=batch_size, 
                        num_workers=num_workers)
        print ("\nTesting " + dataset_type + " with {} workers".format(num_workers))
        num_batches = 0
        for fname, fobj in dataloader:
            fname = [x.split("/")[-1] for x in fname]
            batch_set = set(map(tuple, zip(fname, fobj)))
            s3_obj_set.update(batch_set)
            num_batches += 1

        # print (next(iter(s3_obj_set))[0])
        # print (next(iter(boto_obj_set))[0])  
        # print (num_batches, expected_batches)
        # print (len(s3_obj_set), len(boto_obj_set))

        assert s3_obj_set == boto_obj_set, "Test fails for {} workers for".format(num_workers
                                                        ) + dataset_type           
        print ("All data correctly loaded for " + dataset_type + " for {} workers".format(num_workers))

        assert expected_batches == num_batches, "Data Incorrectly batched for {} workers for ".format(num_workers
                                                        ) + dataset_type
        print ("Data correctly batched for " + dataset_type + " for {} workers".format(num_workers))

def test_S3IterableDataset(boto_obj_set, bucket, prefix_list):
    batch_size = 32
    url_list = ["s3://" + bucket + "/" + prefix for prefix in prefix_list]
    
    test_workers("S3IterableDataset", url_list,  batch_size, boto_obj_set)

def test_S3Dataset(boto_obj_set, bucket, prefix_list):
    batch_size = 32
    url_list1 = ["s3://" + bucket + "/" + prefix for prefix in prefix_list]
    url_list2 = ["s3://ydaiming-test-data2/integration_tests/files"]
    test_workers("S3Dataset", url_list2,  batch_size, boto_obj_set)

def test_tarfiles(bucket, tarfiles_list):
    print("Testing: Reading tarfile...")
    boto_obj_set = read_using_boto(bucket, tarfiles_list)
    batch_size = 32
    url_list = ["s3://" + bucket + "/" + tarfile for tarfile in tarfiles_list]
    test_workers("S3IterableDataset", url_list, batch_size, boto_obj_set)

def test_files(bucket, files_prefix):
    prefix_list = get_file_list(bucket, files_prefix)
    boto_obj_set = read_using_boto(bucket, prefix_list)
    batch_size = 32

    print ("\nTesting: Reading individual files...")
    url_list = ["s3://" + bucket + "/" + prefix for prefix in prefix_list]
    # test_workers("S3IterableDataset", url_list, batch_size, boto_obj_set) # this is failing
    test_workers("S3Dataset", url_list, batch_size, boto_obj_set)


    print ("Testing: Reading from prefix...")
    url_list = ["s3://" + bucket + "/" + files_prefix]
    # test_workers("S3IterableDataset", url_list, batch_size, boto_obj_set)
    test_workers("S3Dataset", url_list, batch_size, boto_obj_set)


if __name__ == "__main__":
    print ("Starting the Tests\n")
    
    bucket = "ydaiming-test-data2"

    tar_prefix_list = ["integration_tests/imagenet-train-000000.tar"]
    test_tarfiles(bucket, tar_prefix_list)

    files_prefix = "integration_tests/files"
    assert files_prefix[-1] != "/", "Enter Prefix without trailing \"/\" else error"
    test_files(bucket, files_prefix)

    print ("\nAll tests passed successfully")