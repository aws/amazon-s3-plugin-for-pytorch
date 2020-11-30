import io
import math
import boto3
from collections import defaultdict
from torch.utils.data import DataLoader

from awsio.python.lib.io.s3.s3dataset import S3Dataset, S3IterableDataset
from awsio.python.lib.io.s3.s3dataset import tardata, zipdata

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

    dataset_class = eval(dataset_type)
    for num_workers in [ 0, 4, 16]:
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

        assert s3_obj_set == boto_obj_set, "Test fails for {} workers for".format(num_workers
                                                        ) + dataset_type           
        print ("All data correctly loaded for " + dataset_type + " for {} workers".format(num_workers))

def test_tarfiles(bucket, tarfiles_list):
    print("\nINITIATING: TARFILES READ TEST")
    boto_obj_set = read_using_boto(bucket, tarfiles_list)
    batch_size = 32
    url_list = ["s3://" + bucket + "/" + tarfile for tarfile in tarfiles_list]
    test_workers("S3IterableDataset", url_list, batch_size, boto_obj_set)

def test_files(bucket, files_prefix):
    prefix_list = get_file_list(bucket, files_prefix)
    boto_obj_set = read_using_boto(bucket, prefix_list)
    batch_size = 32

    print ("\nINITIATING: INDIVIDUAL FILE READ TEST")
    url_list = ["s3://" + bucket + "/" + prefix for prefix in prefix_list]
    test_workers("S3IterableDataset", url_list, batch_size, boto_obj_set)
    test_workers("S3Dataset", url_list, batch_size, boto_obj_set)

    print ("\nINITIATING: READ FILES FROM PREFIX TEST")
    url_list = ["s3://" + bucket + "/" + files_prefix]
    test_workers("S3IterableDataset", url_list, batch_size, boto_obj_set)
    test_workers("S3Dataset", url_list, batch_size, boto_obj_set)

def test_shuffle(bucket, files_prefix):
    prefix_list = get_file_list(bucket, files_prefix)
    url_list = ["s3://" + bucket + "/" + prefix for prefix in prefix_list]
    batch_size = 32
    shuffled_sets = defaultdict(set)
    shuffled_lists = defaultdict(list)

    print ("\nINITIATING SHUFFLE TEST")
    for shuffle_urls in [True, False]:
        dataset = S3IterableDataset(url_list, shuffle_urls=shuffle_urls)
        dataloader = DataLoader(dataset,
                        batch_size=batch_size)
        
        for fname, fobj in dataloader:
            fname = [x.split("/")[-1] for x in fname]
            batch_set = set(map(tuple, zip(fname, fobj)))
            batch_list = list(map(tuple, zip(fname, fobj)))
            shuffled_sets[str(shuffle_urls)].update(batch_set)
            shuffled_lists[str(shuffle_urls)].append(batch_list)
    assert shuffled_sets['True'] == shuffled_sets['False'] and shuffled_lists['True'] != shuffled_lists['False'], \
            "Shuffling not working correctly"
    print ("Shuffle test passed for S3IterableDataset")

if __name__ == "__main__":
    print ("Starting the Tests\n")
    
    bucket = "ydaiming-test-data2"

    tar_prefix_list = ["integration_tests/imagenet-train-000000.tar"]
    test_tarfiles(bucket, tar_prefix_list)

    files_prefix = "integration_tests/files"
    assert files_prefix[-1] != "/", "Enter Prefix without trailing \"/\" else error"
    test_files(bucket, files_prefix)

    test_shuffle(bucket, files_prefix)

    print ("\nAll tests passed successfully")