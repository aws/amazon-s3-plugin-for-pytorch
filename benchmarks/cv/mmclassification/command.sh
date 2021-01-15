#!/bin/bash 
python -m torch.distributed.launch --nproc_per_node=8 --nnodes=1 --node_rank=0  tools/train.py configs/imagenetfsx/resnet50_b64.py --work-dir=work_dir/ibr_tar --launcher="pytorch" --no-validate
