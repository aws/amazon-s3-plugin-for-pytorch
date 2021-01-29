# S3 Plugin

S3-plugin is a high performance PyTorch dataset library to efficiently access datasets stored in S3 buckets. It provides steaming data access to datasets of any size and thus eliminates the need to provision local storage capacity. The library is designed to leverage the high throughput that S3 offers to access objects with minimal latency.

The users have the flexibility to use either map-style or iterable-style dataset interfaces based on their needs. The library itself is file-format agnostic and presents objects in S3 as a binary buffer(blob). Users are free to apply any additional transformation on the data received from S3.

## Installation

You can install this package by following the below instructions.

#### Prerequisite

- Python 3.6 (or Python 3.7) is required for this installation. Instructions are written for Python 3.6 but are almost identical for Python 3.7 (changes needed: 1. Python version when creating Conda environment; 2. Python version references in the URL to the wheel). We recommend that you create a Conda environment:

```shell
conda create --name py36 python=3.6
conda activate py36
```

- Install AWS-SDK-CPP (https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/setup.html). Make sure to select the build type as ‘Release’ and BUILD_ONLY param as ‘s3;transfer’ as we only need s3 and transfer components:

```shell
git clone https://github.com/aws/aws-sdk-cpp.git
cd aws-sdk-cpp
cmake . -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=$HOME/bin/aws-sdk -D BUILD_ONLY="s3;transfer"
make
make install
```

- Pytorch >= 1.5

#### Installing via Wheel

```shell script
# TODO Add final public wheels
aws s3 cp <S3 URI> .
pip install <whl name awsio-0.0.1-cp...whl>
```

### Configuration

Before reading data from S3 bucket, you need to provide bucket region parameter:

* `AWS_REGION`: By default, regional endpoint is used for S3, with region controlled by `AWS_REGION`. If `AWS_REGION` is not specified, then `us-west-2` is used by defaule.

To read objects in a bucket that is not publicly accessible, AWS credentials must be provided through one of the following methods:

* Set credentials in the AWS credentials profile file on the local system, located at: `~/.aws/credentials` on Linux, macOS, or Unix
* Set the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables.
* If you are using this library on an EC2 instance, specify an IAM role and then give the EC2 instance access to that role.

#### Smoke Test
To test your setup, run:
```
bash tests/smoke_tests/import_awsio.sh
```

The test will first make sure that the package imports correctly by printing the commit hash related to the build.
Then, it will prompt the user for a S3 url to a file and return whether or not the file exists.

For example:
```
$ bash tests/smoke_tests/import_awsio.sh 
Testing: import awsio
0.0.1+b119a6d
import awsio succeeded
S3 URL : 's3://ydaiming-test-data2/test_0.JPEG'
Testing: checking setup by quering whether or not 's3://ydaiming-test-data2/test_0.JPEG' is an existing file
file_exists: True
Smoke test was successful.
```

### Usage

Once the above setup is complete, you can interact with S3 bucket in following ways:
Accepted input S3 url formats:

* Single url 

* `url = 's3://path/to/bucket/abc.tfrecord'`

* List of urls as follows:

```urls = ['s3://path/to/bucket/abc.tfrecord','s3://path/to/bucket/def.tfrecord']```

* Prefix to S3 bucket to include all files under 's3_prefix' folder starting with '0'

```urls = 's3://path/to/s3_prefix/0'```
  
* Using `list_files()` function, which can be used to manipulate input list of urls to fetch as follows:
```shell
from awsio.python.lib.io.s3.s3dataset import list_files
urls = list_files('s3://path/to/s3_prefix/0')
```

#### Map-Style Dataset

If each object in S3 contains a single training sample, then map-style dataset i.e. S3Dataset can be used. To partition data across nodes and to shuffle data, this dataset can be used with PyTorch distributed sampler. Additionally, pre-processing can be applied to the data in S3 by extending the S3Dataset class. Following example illustrates use of map-style S3Dataset for image datasets: 

```python
from awsio.python.lib.io.s3.s3dataset import S3Dataset
from torch.utils.data import DataLoader
from torchvision import transforms
from PIL import Image
import io

class S3ImageSet(S3Dataset):
    def __init__(self, urls, transform=None):
        super().__init__(urls)
        self.transform = transform

    def __getitem__(self, idx):
        img_name, img = super(S3ImageSet, self).__getitem__(idx)
        # Convert bytes object to image
        img = Image.open(io.BytesIO(img)).convert('RGB')
        
        # Apply preprocessing functions on data
        if self.transform is not None:
            img = self.transform(img)
        return img

batch_size = 32

preproc = transforms.Compose([
    transforms.ToTensor(),
    transforms.Normalize((0.485, 0.456, 0.406), (0.229, 0.224, 0.225)),
    transforms.Resize((100, 100))
])

# urls can be S3 prefix containing images or list of all individual S3 images
urls = 's3://path/to/s3_prefix/'

dataset = S3ImageSet(urls, transform=preproc)
dataloader = DataLoader(dataset,
        batch_size=batch_size,
        num_workers=64)

```


#### Iterable-style dataset

If each object in S3 contains multiple training samples e.g. archive files containing multiple small images or TF record files/shards containing multiple records, then it is advisable to use the Iterable-style dataset implementation i.e. S3IterableDataset. For the specific case of zip/tar archival files, each file contained in the archival is returned during each iteration in a streaming fashion. For all other file formats, binary blob for the whole shard is returned and users need to implement the appropriate parsing logic. Besides, S3IterableDataset takes care of partitioning the data across nodes and workers in a distributed setting.

`Note:` For datasets consisting of a large number of smaller objects, accessing each object individually can be inefficient. For such datasets, it is recommended to create shards of the training data and use S3IterableDataset for better performance.
```shell
# tar file containing label and image files as below
 tar --list --file=file1.tar |  sed 4q

1234.cls
1234.jpg
5678.cls
5678.jpg
```

Consider tar file for image classification. It can be easily loaded by writing a custom python generator function using the iterator returned by S3IterableDataset. (Note: To create shards from a file dataset refer this [link](https://github.com/tmbdev/pytorch-imagenet-wds).)


```python
from torch.utils.data import IterableDataset
from awsio.python.lib.io.s3.s3dataset import S3IterableDataset
from PIL import Image
import io
import numpy as np
from torchvision import transforms

class ImageS3(IterableDataset):
    def __init__(self, urls, shuffle_urls=False, transform=None):
        self.s3_iter_dataset = S3IterableDataset(urls,
                                                 shuffle_urls)
        self.transform = transform

    def data_generator(self):
        try:
            while True:
                # Based on alphabetical order of files, sequence of label and image may change.
                label_fname, label_fobj = next(self.s3_iter_dataset_iterator)
                image_fname, image_fobj = next(self.s3_iter_dataset_iterator)
                
                label = int(label_fobj)
                image_np = Image.open(io.BytesIO(image_fobj)).convert('RGB')
                
                # Apply torch vision transforms if provided
                if self.transform is not None:
                    image_np = self.transform(image_np)
                yield image_np, label

        except StopIteration:
            return
            
    def __iter__(self):
        self.s3_iter_dataset_iterator = iter(self.s3_iter_dataset)
        return self.data_generator()
        
    def set_epoch(self, epoch):
        self.s3_iter_dataset.set_epoch(epoch)

# urls can be a S3 prefix containing all the shards or a list of S3 paths for all the shards 
 urls = ["s3://path/to/file1.tar", "s3://path/to/file2.tar"]

# Example Torchvision transforms to apply on data    
preproc = transforms.Compose([
    transforms.ToTensor(),
    transforms.Normalize((0.485, 0.456, 0.406), (0.229, 0.224, 0.225)),
    transforms.Resize((100, 100))
])

dataset = ImageS3(urls, transform=preproc)

```
 
This dataset can be easily used with dataloader for parallel data loading and preprocessing:

```python
dataloader = torch.utils.data.DataLoader(dataset, num_workers=4, batch_size=32)
```

We can shuffle the sequence of fetching shards by setting shuffle_urls=True and calling set_epoch method at the beginning of every epochs as:
```python
dataset = ImageS3(urls, transform=preproc, shuffle_urls=True)
for epoch in range(epochs):
    dataset.set_epoch(epoch)
    # training code ...
```

Note that the above code will only shuffle sequence of shards, the individual training samples within shards will be fetched in the same order. To shuffle the order of training samples across shards, use ShuffleDataset. ShuffleDataset maintains a buffer of data samples read from multiple shards and returns a random sample from it. The count of samples to be buffered is specified by buffer_size. To use ShuffleDataset, update the above example as follows:

```python
dataset = ShuffleDataset(ImageS3(urls), buffer_size=4000)
```

#### Iterable-style dataset (NLP)
The data set can be similarly used for NLP tasks. Following example demonstrates use for S3IterableDataset for BERT data loading. 

```shell script
# Consider S3 prefix containing hdf5 files.
# Each hdf5 file contains numpy arrays for different variables required for BERT 
# training such as next sentence labels, masks etc.
aws s3 ls --human-readable s3://path/to/s3_prefix |  sed 3q


file_1.hdf5
file_2.hdf5
file_3.hdf5

```

```python

import torch
from torch.utils.data import IterableDataset, DataLoader
from itertools import islice
import h5py
import numpy as np
import io
from awsio.python.lib.io.s3.s3dataset import S3IterableDataset

def create_data_samples_from_file(fileobj):
    # Converts bytes data to numpy arrays
    keys = ['input_ids', 'input_mask', 'segment_ids', \
        'masked_lm_positions', 'masked_lm_ids', 'next_sentence_labels']
    dataset = io.BytesIO(fileobj)
    with h5py.File(dataset, "r") as f:
        data_file = [np.asarray(f[key][:]) for key in keys]
    return data_file

class s3_dataset(IterableDataset):

    def __init__(self, urls):
        self.urls = urls
        self.dataset = S3IterableDataset(self.urls, shuffle_urls=True)

    def data_generator(self):
        try:
            while True:
                filename, fileobj = next(self.dataset_iter)
                # data_samples: list of six numpy arrays 
                data_samples = create_data_samples_from_file(fileobj)
                
                for sample in list(zip(*data_samples)):
                    # Preprocess sample if required and then yield
                    yield sample

        except StopIteration as e:
            return

    def __iter__(self):
        self.dataset_iter = iter(self.dataset)
        return self.data_generator()

urls = "s3://path/to/s3_prefix"
train_dataset = s3_dataset(urls)

```

### Test Coverage

To check python test coverage, install [`coverage.py`](https://coverage.readthedocs.io/en/latest/index.html) as follows:

```
pip install coverage
```

To make sure that all tests are run, please also install `pytest`, `boto3`, and `pandas` as follows:
```
pip install pytest boto3 pandas
``` 

To run tests and calculate coverage:

```asm
coverage erase
coverage run -p --source=awsio -m pytest -v tests/py-tests/test_regions.py \
tests/py-tests/test_utils.py \
tests/py-tests/test_s3dataset.py \
tests/py-tests/test_s3iterabledataset.py \
tests/py-tests/test_read_datasets.py \
tests/py-tests/test_integration.py
coverage combine
coverage report -m
```
