import os
import io
import boto3
import numpy as np
from random import sample
from time import time

from awsio.python.lib.io.s3.s3dataset import S3Dataset, S3IterableDataset
import h5py # must be version 2.9.0 or newer
from torch.utils.data import IterableDataset, DataLoader

def fileobj_to_np_list(fileobj, is_s3=True):
    KEYS = ['input_ids', 'input_mask', 'segment_ids', 'masked_lm_positions', 'masked_lm_ids', 'next_sentence_labels']
    if is_s3:
        fileobj = io.BytesIO(fileobj)
    data_file = []
    with h5py.File(fileobj, 'r') as f:
        data_file = [np.asarray(f[key][:]) for key in KEYS]
    return data_file

def generate_data_boto3(filename):
    """
    Return list of numpy arrays created by first saving the idx-th hdf5 file
    locally using boto3.
    """
    # inputs for boto3 s3 download
    bucket = filename.split('/')[2]
    key = os.path.join(*filename.split('/')[3:])
    output_filename = '/tmp/input_file.hdf5'

    # download file from s3
    s3 = boto3.client('s3')
    s3.download_file(bucket, key, output_filename)

    # create list of numpy arrays
    return fileobj_to_np_list(output_filename, is_s3=False)

def test_s3dataset(s3_path, num_samples=5):
    s3_dataset = S3Dataset(s3_path)
    # pick five random files to test
    random_idx = sample(range(0, len(s3_dataset)), num_samples)
    total_time = 0
    for idx in random_idx:
        start = time()
        filename, fileobj = s3_dataset[idx]
        print("Testing S3Dataset with file", filename)
        s3_result = fileobj_to_np_list(fileobj)
        end = time()
        total_time += end - start
        boto3_result = generate_data_boto3(filename)
        assert all([np.allclose(s3_elem, boto3_elem) for s3_elem, boto3_elem in zip(s3_result, boto3_result)])
    print("Average time for S3Dataset :", total_time / num_samples)

def test_s3iterable(s3_path, num_samples=5):
    start = time()
    dataset = S3IterableDataset(s3_path, shuffle_urls=True)
    end = time()
    total_time = end - start
    print("Time to initialize S3IterableDataset :", total_time)
    tmp = 0
    for data in dataset:
        # test only 5 random files
        tmp += 1
        if tmp > num_samples:
            break
        start = time()
        filename, fileobj = data
        print("Testing S3IterableDataset with file", filename)
        s3_result = fileobj_to_np_list(fileobj)
        end = time()
        total_time += end - start
        boto3_result = generate_data_boto3(filename)
        assert all ([np.allclose(s3_elem, boto3_elem) for s3_elem, boto3_elem in zip (s3_result, boto3_result)])
    print("Average time for S3IterableDataset :", total_time / num_samples)

class s3_dataset(IterableDataset):
    def __init__(self, path):
        self.s3_directory=path

    def file_generator(self):
        try:
            while True:
                filename, fileobj = next(self.dataset_iter)
                data_samples = fileobj_to_np_list(fileobj, is_s3=True)
                data_samples_transpose = list(zip(*data_samples))
                count = 0
                for data in data_samples_transpose:
                    if count % 10000 == 0:
                        print(count, filename)
                    count += 1
                    yield data
        except StopIteration as e:
            raise e

    def __iter__(self):
        self.dataset = S3IterableDataset(self.s3_directory)
        self.dataset_iter = iter(self.dataset)
        return self.file_generator()
                
def test_iterable_generator(s3_path):
    dataset = s3_dataset(s3_path)
    train_dataloader = DataLoader(dataset, pin_memory=True)
    count = 0
    for data in train_dataloader:
        if count % 1000 == 0:
            print(type(data))
        count += 1


if __name__ == '__main__':
    s3_path = 's3://choidong-bert/phase1/training/wiki_books_corpus_training'
    test_s3dataset(s3_path)
    test_s3iterable(s3_path)
    test_iterable_generator(s3_path)

