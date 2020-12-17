# aws_io

## Dependencies
- Pytorch 1.5
- AWS-SDK-CPP (core and S3) See below for instructions
- cmake (>v3.2)
- python development libraries(including pybind 11) (conda install pybind11)


### Adding AWS-SDK-CPP as a dependency

1) Install AWS-SDK-CPP [Preferred Approach]

Note: Only install s3 and transfer components as mentioned below.

https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/setup.html

Building and installing whole package takes few hours to build so just added S3 plugin which we need for this project.

```
cmake . -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=$HOME/bin/aws-sdk -D BUILD_ONLY="s3;transfer"
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

```
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

```
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

### Smoke Test
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
