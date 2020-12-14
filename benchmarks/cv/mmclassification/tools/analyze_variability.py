import json
# import boto3
import os
import os.path as osp
import statistics
from collections import defaultdict
import argparse
from mmcv import Config
import sys
import pandas as pd
sys.path.append(os.getcwd())
sys.path.append( '.' )
sys.path.append(osp.join(os.getcwd(),'../' ) )



def parse_args():
    parser = argparse.ArgumentParser(description='Analyze variability')
    parser.add_argument('--metrics_csv_file', default=None, help='train config file path')
    args = parser.parse_args()
    return args


def main():
    args = parse_args()

    metrics_csv_file = args.metrics_csv_file
    df = pd.read_csv(metrics_csv_file)
    df.describe()

if __name__ == '__main__':
    main()

