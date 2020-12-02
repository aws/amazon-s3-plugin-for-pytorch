import torch
from torch.utils.data import IterableDataset, DataLoader

# data is in hdf5 format and converted to numpy
import h5py
import numpy as np

# packages for this example
import io
from awsio.python.lib.io.s3.s3dataset import S3IterableDataset

def create_data_samples_from_file(fileobj):
    """Convert bytes from S3IterableDataset to numpy arrays.
    Helper function for class s3_dataset.

    Returns a list of six numpy arrays which each contain
    data (by key) for all samples in a file.

    Keyword arguments:
    fileobj -- the bytes string provided by S3IterableDataset        
    """
    data_file = []
    keys = ['input_ids', 'input_mask', 'segment_ids', \
        'masked_lm_positions', 'masked_lm_ids', 'next_sentence_labels']
    dataset = io.BytesIO(fileobj)
    with h5py.File(dataset, "r") as f:
        data_file = [np.asarray(f[key][:]) for key in keys]
    return data_file


class s3_dataset(IterableDataset):
    """Dataset used for training.
    Each file contains approximately 150K samples/file.
    Yields one formatted sample.
    """
    def __init__(self, max_pred_length):
        self.s3_directory = "s3://choidong-bert/phase1/training/wiki_books_corpus_training"
        self.max_pred_length = max_pred_length

    def data_generator(self):
        try:
            while True:
                filename, fileobj = next(self.dataset_iter)
                # data_samples: list of six numpy arrays (each array contains all samples)
                data_samples = create_data_samples_from_file(fileobj)
                # transpose data_samples so that each index represents one sample
                data_sample_transpose = list(zip(*data_samples))
                random.shuffle(data_sample_transpose)
                for sample in data_sample_transpose:
                    yield sample

        except StopIteration as e:
            raise e

    def __iter__(self):
        self.dataset = S3IterableDataset(self.s3_directory, shuffle_urls=True)
        self.dataset_iter = iter(self.dataset)
        return self.data_generator()


def main():
    train_dataset = s3_dataset(max_pred_length=max_predictions_per_seq)
    train_dataloader = DataLoader(train_dataset, pin_memory=True)
    for step, sample in enumerate(train_dataloader):
        training_steps += 1
        sample = [elem.to(device) for elem in sample]
        input_ids, segment_ids, input_mask, masked_lm_labels, next_sentence_labels = sample
