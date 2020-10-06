import pytest
import torch
from awsio.python.lib.io.s3.s3dataset import S3Dataset, S3IterableDataset
from awsio.python.lib.io.s3.s3dataset import list_files, file_exists, get_file_size



def test_num_workers() :
    torch.backends.cudnn.benchmark = True

    # Parameters
    params = {'batch_size': 1,
	      'shuffle': True,
	      'num_workers': 64}
    max_epochs = 1



    # Generators
    # training_set = S3Dataset('s3://coco-dataset-small/train2017')
    #path = 's3://choidong-test-data/wikitext-2'
    path = 's3://ydaiming-test-data2/folder_1'
    path = 's3://choidong-test-data/wikitext-2'
    path = 's3://coco-dataset-small/train2017'
    path = 's3://coco-dataset-mmdetection/train2017'
    path = 's3://choidong-bert/phase2/training'

    training_set = S3Dataset(path)

    files = list_files(path)
    print("length:" + str(len(files)))
    print("Traing set len is {0}".format(training_set.__len__()))

    print(training_set.urls_list)
    # training_set = S3IterableDataset(path)
    training_generator = torch.utils.data.DataLoader(training_set, **params)

    # Loop over epochs
    for epoch in range(max_epochs):
        # Training
        count = 0
        for local_batch in training_generator:
            # Transfer to GPU
            count+=1
        
        print("count is :{0}".format(count))

test_num_workers()
