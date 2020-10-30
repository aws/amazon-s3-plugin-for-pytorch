_base_ = [
    '../_base_/models/resnet50.py', '../_base_/datasets/imagenets3_bs64.py',
    '../_base_/schedules/imagenet_bs2048_coslr_s3.py', '../_base_/default_runtime.py'
]
