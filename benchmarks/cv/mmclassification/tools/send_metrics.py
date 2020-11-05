import json
import boto3
import os
import os.path as osp
import statistics
from collections import defaultdict
import argparse
from mmcv import Config
import sys
sys.path.append(os.getcwd())
sys.path.append( '.' )
sys.path.append(osp.join(os.getcwd(),'../' ) )

def load_json_logs(json_logs):
    # load and convert json_logs to log_dict, key is epoch, value is a sub dict
    # keys of sub dict is different metrics, e.g. memory, bbox_mAP
    # value of sub dict is a list of corresponding values of all iterations
    log_dicts = [dict() for _ in json_logs]
    for json_log, log_dict in zip(json_logs, log_dicts):
        with open(json_log, 'r') as log_file:
            for line in log_file:
                log = json.loads(line.strip())
                # skip lines without `epoch` field
                if 'epoch' not in log:
                    continue
                epoch = log.pop('epoch')
                if epoch not in log_dict:
                    log_dict[epoch] = defaultdict(list)
                for k, v in log.items():
                    log_dict[epoch][k].append(v)
    return log_dicts


def get_metrics(json_logs, num_gpus, epoch_num, model, suffix="", batch_size=None):
    '''
    Parse through log json and fetch training metrics into dict
    '''

    for json_log in json_logs:
        assert json_log.endswith('.json')
    log_dicts = load_json_logs(json_logs)
    stats = {}

    memory = statistics.mean(log_dicts[0][epoch_num]['memory']) / 1024.0
    stats["Memory" + suffix] = {}
    stats["Memory" + suffix]["Unit"] = "Gigabytes"
    stats["Memory" + suffix]["Value"] = memory

    # TODO: Pass batch size as arguement
    if not batch_size:
        batch_size = 32 # Setting previous default value
    throughput = 1.0 / (statistics.mean(log_dicts[0][epoch_num]['time']) / (num_gpus * batch_size))
    stats["Throughput" + suffix] = {}
    stats["Throughput" + suffix]["Unit"] = "Count/Second"
    stats["Throughput" + suffix]["Value"] = throughput

    if 'top-1' in log_dicts[0][epoch_num]:

        acc1 = log_dicts[0][epoch_num]['top-1'][-1]
        stats["top-1" + suffix] = {}
        stats["top-1" + suffix]["Unit"] = "None"
        stats["top-1" + suffix]["Value"] = acc1

    if 'top-5' in log_dicts[0][epoch_num]:
        acc5 = log_dicts[0][epoch_num]['top-5'][-1]
        stats["top-5" + suffix] = {}
        stats["top-5" + suffix]["Unit"] = "None"
        stats["top-5" + suffix]["Value"] = acc5

    return stats


def get_last_log(directory):
    jsons = []
    for file in os.listdir(directory):
        if (file.endswith(".json")):
            jsons.append(os.path.join(directory, file))

    return [sorted(jsons)[-1]]


def _get_time(directory):
    filename = os.path.join(directory, "timetaken")
    time = open(filename, 'r').read().split('\n')

    return int(time[0])


def _convert_model_name(model):
    '''
    Making sure the existing CloudWatch metric name matches
    '''

    return model


def parse_args():
    parser = argparse.ArgumentParser(description='Push training metrics to CloudWatch')
    parser.add_argument('num_gpus', type=int, help='Number of GPUs')
    parser.add_argument('work_dir', help='the dir that has the logs')
    parser.add_argument('model_name', help='model name')
    parser.add_argument('epoch_num', type=int, help='number of epochs')
    parser.add_argument('--run_herring', type=int, default=0, help='enable herring')
    parser.add_argument('config', default=None, help='train config file path')
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    work_dir = args.work_dir
    json_logs = get_last_log(work_dir)
    model = args.model_name
    batch_size = None
    if args.run_herring:
        suffix = "-Herring"
    else:
        suffix = ""
    if args.config:
        args.config = '../' +  args.config
        cfg = Config.fromfile(args.config)
        batch_size = cfg.get('data')['samples_per_gpu']


    stats = get_metrics(json_logs, args.num_gpus, args.epoch_num, model, suffix, batch_size)

    stats["Total Time" + suffix] = {}
    stats["Total Time" + suffix]["Unit"] = "Minutes"
    stats["Total Time" + suffix]["Value"] = _get_time(work_dir) /60.0

    stats["Time per Epoch" + suffix] = {}
    stats["Time per Epoch" + suffix]["Unit"] = "Minutes"
    stats["Time per Epoch" + suffix]["Value"] = stats["Total Time"]["Value"] / (args.epoch_num)
    print(stats)

    cloudwatch = boto3.client('cloudwatch', region_name='us-west-2')

    full_run_metrics = ["top-1", "top-5", "Time per Epoch", "Total Time"]
    quick_run_metrics = ["Throughput", "Memory"]
    Data = []

    model = _convert_model_name(model)
    namespace = model

    for metric in full_run_metrics:
        try:
            metric_data = {}
            metric_data['MetricName'] = metric
            metric_data['Unit'] = stats[metric]["Unit"]
            metric_data["Value"] = stats[metric]["Value"]
            Data.append(metric_data)
        except:
            pass

    for metric in quick_run_metrics:
        try:
            metric_data = {}
            metric_data['MetricName'] = metric
            metric_data['Unit'] = stats[metric]["Unit"]
            metric_data["Value"] = stats[metric]["Value"]
            Data.append(metric_data)
        except:
            pass

    print("Data: ", Data)

if __name__ == '__main__':
    main()

