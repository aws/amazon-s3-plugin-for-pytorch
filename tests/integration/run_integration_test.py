import boto3
import logging
import subprocess
import time
import traceback
from fabric.api import env, put, run

from integ_test_cfg import job

logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y/%m/%d %I:%M:%S%p', level=logging.INFO)


def create_key_pair(ec2):
    '''Create a file to store the key pair locally.

    This function will first check for and delete any key pair 
    with the same key name. It will also delete the pem file 
    with the same name. Finally, it will give the user read 
    permission to the newly created pem file.
    '''
    pem_key = job['key_pair_filename']
    logging.info(f"Creating {pem_key}")
    _ = ec2.delete_key_pair(KeyName=job['key_name'])
    key_pair = ec2.create_key_pair(KeyName=job['key_name'])
    key_pair_str = str(key_pair['KeyMaterial'])
    subprocess.run(f'rm -rf {pem_key}', shell=True)
    with open(pem_key, 'w') as outfile:
        outfile.write(key_pair_str)
    subprocess.run(f'chmod 400 {pem_key}', shell=True)


def launch_instance():
    '''Launches one EC2 instance, returns the instance ID in a list.
    Waits for the instance to be in running state.
    Returns the instance ID and public DNS.
    '''
    logging.info("Launching instance")
    try:
        ec2 = boto3.resource('ec2')
        # start instance
        instances = ec2.create_instances(
            ImageId=job['ami_id'],
            MinCount=1,
            MaxCount=1,
            InstanceType=job['instance_type'],
            KeyName=job['key_name'],
            IamInstanceProfile={'Name': job['iam_role']},
            TagSpecifications=[{'ResourceType': 'instance',
                                'Tags': [{'Key': 'Name', 'Value': job['key_name']}]
                                }]
        )

        # update instance attributes
        logging.info("Waiting for instance to be in running state")
        instance = instances[0]
        instance.wait_until_running()
        instance.load()

        instance_id = instance.instance_id
        public_dns = instance.public_dns_name
        return instance_id, public_dns

    except Exception as e:
        print(e)


def run_commands(public_dns):
    '''Run the integration test.

    Initializes fabric and waits for SSH connection.
    Copies start_test.sh to EC2 instance and runs it.
    '''
    # initialize env for fabric
    env.host_string = public_dns
    env.user = 'ubuntu'
    env.key_filename = job['key_pair_filename']
    _wait_for_ssh()

    logging.info(f'Copy start_test.sh to {public_dns}')
    put('start_test.sh', '~/')

    ret = run('bash start_test.sh', quiet=False)
    if ret.return_code != 0:
        logging.warn(f'ERROR: Failed to run command bash script')
        return False

    return True


def _wait_for_ssh(tries=120):
    '''Wait for ssh server to be up and listening on the instance.
    '''
    logging.info('Waiting for SSH server connection')
    num_attempts = 1
    successful = False
    while not successful:
        try:
            run('whoami', quiet=True)
            successful = True
        except:
            num_attempts += 1
            if num_attempts > tries:
                print('ERROR: Timeout while waiting for SSH server')
                return
            else:
                time.sleep(5)


if __name__ == '__main__':
    subprocess.run('clear', shell=True)
    boto3.setup_default_session(region_name=job['region'])
    try:
        # launch instance
        ec2 = boto3.client('ec2')
        create_key_pair(ec2)
        instance_id, public_dns = launch_instance()

        # upload and run start_test.sh
        response = run_commands(public_dns)
        if response:
            logging.info("Job was successful")
        else:
            logging.warn("Job failed")
    except Exception as e:
        logging.warn(f'ERROR: {e}')
        logging.warn(traceback.format_exc())
    finally:
        logging.info(f'Terminating instance {instance_id}')
        response = ec2.terminate_instances(InstanceIds=[instance_id])
