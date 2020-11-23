# S3 Plugin

S3 plugin is a PyTorch Iterable dataset which allows to train pytorch models by streaming data residing in S3 bucket. 
It supports all the file formats in S3. It enables users to train models on large scale data without having to worry about
copying data on local nodes. S3 plugin can be used as:

```python
from torch.aws_io import S3IterableDataset
from torchvision import transforms, utils

normalize = transforms.Normalize(
    mean=[0.485, 0.456, 0.406],
    std=[0.229, 0.224, 0.225])
    
preproc = transforms.Compose([
    transforms.RandomResizedCrop(224),
    transforms.RandomHorizontalFlip(),
    transforms.ToTensor(),
    normalize,
])

# API 
dataset = (
    S3IterableDataset('s3://path/to/image_data/folder or file')
    .dataproc(batch_size=200, num_shards =2) # defines shuffling, sharding, sampling parameters
    .decode("pil")
    .transform_fn(image=preproc)   
)

loader = torch.utils.data.DataLoader(dataset, num_workers=8)
for inputs, targets in loader:
    ... # training

```
### Usage
S3 plugin works with dataset in any format. 
1. Using same format as disk
 

2. Using shards or tar files:


### Creating tar files
Add path to script




## To Do
1. List all file format supported 
2. Can we have public s3 buckets so that the example s3 link will work for all customers?
3. Are we going to add S3 boto dataset as well?
 


## Installation
### Dependencies
- Pytorch 1.5
- AWS-SDK-CPP (core and S3) See below for instructions
- cmake (>v3.2)
- python development libraries(including pybind 11) (conda install pybind11)
- Abseil - https://abseil.io/docs/cpp/quickstart-cmake.html


### Adding AWS-SDK-CPP as a dependency

1) Install AWS-SDK-CPP [Preferred Approach]

Note: Only install s3 and transfer components as mentioned below.

https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/setup.html

Building and installing whole package takes few hours to build so just added S3 plugin which we need for this project.

```
cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=$HOME/bin/aws-sdk -D BUILD_ONLY="s3;transfer"
make install
```

2) Build the SDK from source.

```
> git clone git@github.com:aws/aws-sdk-cpp.git
> cd aws-sdk-cpp
aws-sdk-cpp>
```

I generally recommend using a released version of the SDK to ensure that you’ve checked out something stable.
```
aws-sdk-cpp> git checkout 1.7.328
```

```shell
aws-sdk-cpp> mkdir build
aws-sdk-cpp> cd build
aws-sdk-cpp/build> cmake .. -DCMAKE_BUILD_TYPE=Debug 
aws-sdk-cpp/build> make
```



Few things to take care of while building the project: 
- The find_package arguments changed to AWSSDK (looks for AWSSDKConfig.cmake), REQUIRED (generates a fatal error if AWSSDK is not found), and COMPONENTS, followed by a list of components 
- The BUILD_SHARED_LIBS option changed to being on, because the SDK recently started defaulting to building shared vs. static libraries.
- Installed into a custom location, so any reference to -Daws-sdk-cpp_DIR needed to become -DAWSSDK_DIR because of the new name for AWSSDKConfig.cmake.


To build the project and test the added dependencies:

```shell
mkdir build
cd build

# You need to provide aws-sdk-cpp and its dependent libraries path
cmake -DCMAKE_PREFIX_PATH=/home/ubuntu/bin/aws-sdk/lib/aws-checksums/cmake\;/home/ubuntu/bin/aws-sdk/lib/aws-c-common/cmake\;/home/ubuntu/bin/aws-sdk/lib/aws-c-event-stream/cmake\;/home/ubuntu/bin/aws-sdk/lib/cmake\;/home/ubuntu/anaconda3/envs/pytorch_p36/lib/python3.6/site-packages/torch .

make
```

To run the sample, set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` and `AWS_REGION`.

```
./aws_io
```

