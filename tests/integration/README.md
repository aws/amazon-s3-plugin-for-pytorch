# aws_io Integration Test

## Install dependencies
```
pip install boto3 fabric3
```
# COPY /home/ubuntu/.conda/envs/pytorch/lib/python3.7/site-packages/mmcv /usr/local/bin/mmcv \

## Running Test
The test script `run_integration_test.py` requires a working config file (`integ_test_cfg.py` - not provided on GitHub). `integ_test_cfg.py` should specify a dict labeled `job` with the necessary information for launching an EC2 instance. A template is provided below.

To run the test, run:
```
python run_integration_test.py
```

This will trigger the launch of an EC2 instance. Once the EC2 instance is in a running state, `start_test.sh` is copied into the instance and run. The instance will be terminated once the job is successful.

### integ_test_cfg.py template
```
job = {
    'region': 'us-west-2',
    'ami_id': 'ami-',
    'instance_type': 'p3.16xlarge',
    'iam_role': 'iam_role',
    'key_name': 'key_name',
    'key_pair_filename': 'filename.pem'
}
```
