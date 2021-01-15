#!/bin/bash
python -m torch.distributed.launch --nproc_per_node=8 --nnodes=1 --node_rank=0  tools/train.py configs/imagenet/resnet50_b64fsx.py --work-dir=work_dir/ebr_file --launcher="pytorch" --no-validate
