# aws_io

## Dependencies
- Pytorch 1.5
- AWS-SDK-CPP (core and S3)
- cmake (>v3.2)
- python development libraries(including pybind 11)


### Adding AWS-SDK-CPP as a dependency

1) Build the SDK from source.

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

2) Install AWS-SDK-CPP

Building and installing whole package takes few hours to build so just added S3 plugin which we need for this project.

```
cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=$HOME/bin/aws-sdk -D BUILD_ONLY="s3"
make install
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

### Test Coverage

To check python test coverage, install [`coverage.py`](https://coverage.readthedocs.io/en/latest/index.html) as follows:

```
pip install coverage
```

To run tests and calculate coverage:

```asm
coverage erase
coverage run -p --source=awsio -m pytest  -v tests/py-tests/test_regions.py
coverage run -p --source=awsio -m pytest  -v tests/py-tests/test_s3dataset.py tests/py-tests/test_read_datasets.py 
coverage combine
coverage report -m
```

