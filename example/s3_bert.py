import torch
from torch.utils.data import IterableDataset, DataLoader
from tqdm import tqdm

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
    keys = ['input_ids', 'input_mask', 'segment_ids', \
        'masked_lm_positions', 'masked_lm_ids', 'next_sentence_labels']
    dataset = io.BytesIO(fileobj)

    data_file = []
    with h5py.File(dataset, "r") as f:
        data_file = [np.asarray(f[key][:]) for key in keys]

    return data_file

def format_sample(sample, max_pred_length):
    """Format each sample appropriately for BERT pretraining.
    Helper function for class s3_dataset.

    Returns a list of six numpy arrays (by key) that represent
    one data sample.

    Keyword arguments:
    sample -- list of numpy arrays that represent one data sample
    max_pred_length -- max total of masked tokens in input sequence (int)
    """
    [input_ids, input_mask, segment_ids, masked_lm_positions, masked_lm_ids, next_sentence_labels] = [
        torch.from_numpy(input.astype(np.int64)) if indice < 5 else torch.from_numpy(
            np.asarray(input.astype(np.int64))) for indice, input in enumerate(sample)]

    masked_lm_labels = torch.ones(input_ids.shape, dtype=torch.long) * -1
    index = max_pred_length
    # store number of  masked tokens in index
    padded_mask_indices = (masked_lm_positions == 0).nonzero()
    if len(padded_mask_indices) != 0:
        index = padded_mask_indices[0].item()
    masked_lm_labels[masked_lm_positions[:index]] = masked_lm_ids[:index]

    return [input_ids, segment_ids, input_mask, \
        masked_lm_labels, next_sentence_labels]

class s3_dataset(IterableDataset):
    """Dataset used for training.

    Uses S3IterableDataset to read in each file (~ 150K samples/file).
    Uses create_data_samples_from_file to return bytes from 
    S3IterableDataset to a numpy array of samples.
    Uses a randomized index to shuffle and limit the number 
    of samples used (this second part is optional and was done for time).
    Yields one formatted sample.
    
    For example purposes, we hardcode the S3 directory, but 
    it can be taken as an argument.
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
                # truncating data for time
                truncated_idx = len(data_sample_transpose) // 1000
                data_sample_transpose = data_sample_transpose[:truncated_idx]
                for sample in data_sample_transpose:
                    formatted_sample = format_sample(sample, self.max_pred_length)
                    yield formatted_sample

        except StopIteration as e:
            raise e

    def __iter__(self):
        self.dataset = S3IterableDataset(self.s3_directory, shuffle_urls=True)
        self.dataset_iter = iter(self.dataset)
        return self.data_generator()


def main():
    epoch = 0
    num_train_epochs = 3
    max_predictions_per_seq = 80

    while epoch < num_train_epochs:
        train_dataset = s3_dataset(max_pred_length=max_predictions_per_seq)
        train_dataloader = DataLoader(train_dataset, pin_memory=True)
        train_iter = tqdm(train_dataloader, desc="Iteration") if is_main_process() else train_dataloader
        for step, sample in enumerate(train_iter):
            training_steps += 1
            sample = [elem.to(device) for elem in sample]
            input_ids, segment_ids, input_mask, masked_lm_labels, next_sentence_labels = sample
