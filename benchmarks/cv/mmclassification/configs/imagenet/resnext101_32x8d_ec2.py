"""
Config File for resnext101_32x8d EC2 runs
"""
_base_ = [
    '../_base_/models/resnext101_32x8d.py',
    '../_base_/datasets/imagenet_bs32.py',
    '../_base_/schedules/imagenet_bs256.py', '../_base_/default_runtime.py'
]
