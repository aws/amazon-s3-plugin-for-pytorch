import pytest
import torch
from awsio.python.lib.io.s3.s3dataset import S3Dataset, S3IterableDataset, S3BotoSet
from awsio.python.lib.io.s3.s3dataset import list_files, file_exists, get_file_size



def test_num_workers() :
    torch.backends.cudnn.benchmark = True

    # Parameters
    params = {'batch_size': 10,
	      'shuffle': True,
	      'num_workers': 128}
    max_epochs = 1



    # Generators
    # training_set = S3Dataset('s3://coco-dataset-small/train2017')
    #path = 's3://choidong-test-data/wikitext-2'
    path = 's3://ydaiming-test-data2/folder_1'
    path = 's3://choidong-test-data/wikitext-2'
    path = 's3://coco-dataset-small/train2017'
    path= 's3://shaabhn-bert/NV-data/seq_128_pred_20_dupe_5.tar.gz'
    path = 's3://ydaiming-test-data2/folder_1'
    path = 's3://choidong-bert/phase2/training'
    path = 's3://coco-dataset-mmdetection/train2017'

    #training_set = S3BotoSet(path, 'coco-dataset-mmdetection')
    training_set = S3BotoSet(path, 'coco-dataset-mmdetection')

    files = list_files(path)
    print("length:" + str(len(files)))
    print("Traing set len is {0}".format(training_set.__len__()))

    print(training_set.urls_list)
    print(training_set.__getitem__(1))
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
