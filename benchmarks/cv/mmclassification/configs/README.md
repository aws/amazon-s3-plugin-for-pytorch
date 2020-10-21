## Configs

1. configs/imagenet/resnet50_ec2.py: Config file for ResNet50 EC2 benchmark. Uses following config files for model 
definition, dataset, learning schedule and run time config.
    - _base_/models/resnet50.py
    - _base_/datasets/imagenet_bs64.py
    - _base_/schedules/imagenet_bs2048_coslr.py
    - _base_/default_runtime.py
    
2. configs/imagenet/resnext101_32x8d_ec2.py: Config file for ResNext101 EC2 benchmark. Uses following config files for model 
definition, dataset, learning schedule and run time config.
    - _base_/models/resnext101_32x8d.py
    - _base_/datasets/imagenet_bs32.py
    - _base_/schedules/imagenet_bs256.py
    - _base_/default_runtime.py
        
3. Remaining files are from mmClassification, to be used in future for while integrating different models.
