import os
import io
import boto3
import numpy as np
from random import sample
from awsio.python.lib.io.s3.s3dataset import S3Dataset, list_files
import h5py # must be version 2.9.0 or newer

S3_PATH = 's3://choidong-bert/phase1/training/wiki_books_corpus_training'
S3_FILE_LIST = list_files(S3_PATH)
KEYS = ['input_ids', 'input_mask', 'segment_ids', 'masked_lm_positions', 'masked_lm_ids', 'next_sentence_labels']

def generate_data_s3dataset(idx):
    """
    Return list of numpy arrays created by pulling the idx-th hdf5 file
    using S3Dataset.
    """
    data_file = S3_FILE_LIST[idx]
    s3_dataset = S3Dataset(data_file)
    filename, fileobj = s3_dataset[0]

    dataset = io.BytesIO(fileobj)
    data_file = []
    with h5py.File(dataset, 'r') as f:
        data_file = [np.asarray(f[key][:]) for key in KEYS]
    return data_file

def generate_data_boto(idx):
    """
    Return list of numpy arrays created by first saving the idx-th hdf5 file
    locally using boto3.
    """
    data_file = S3_FILE_LIST[idx]

    # inputs for boto3 s3 download
    bucket = data_file.split('/')[2]
    key = os.path.join(*data_file.split('/')[3:])
    filename = '/tmp/input_file.hdf5'

    # download file from s3
    s3 = boto3.client('s3')
    s3.download_file(bucket, key, filename)

    # create list of numpy arrays
    f = h5py.File(filename, 'r')
    data_file = [np.asarray(f[key][:]) for key in KEYS]
    f.close()
    return data_file

def test_hdf5_to_numpy():
    # pick five random files to test
    random_idx = sample(range(0, len(S3_FILE_LIST)), 5)
    for idx in random_idx:
        print("Testing file", S3_FILE_LIST[idx])
        s3_result = generate_data_s3dataset(idx)
        boto3_result = generate_data_boto(idx)
        assert all([np.allclose(s3_elem, boto3_elem) for s3_elem, boto3_elem in zip(s3_result, boto3_result)])

if __name__ == '__main__':
    test_hdf5_to_numpy()

