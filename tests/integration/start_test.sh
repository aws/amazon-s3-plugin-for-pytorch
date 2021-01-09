#!/usr/bin/env bash
S3_WHL="https://choidong-dev.s3-us-west-2.amazonaws.com/s3/binaries/awsio-0.0.1%2Bb155354-cp37-cp37m-manylinux1_x86_64.whl"
NUM_GPU=8

TIMESTAMP=`date "+%Y%m%d-%H%M%S"`
LOG_DIR="s3://choidong-dev/s3/integ_test/$TIMESTAMP/"

source ~/.conda/etc/profile.d/conda.sh
conda activate pytorch
echo $(pip3 list)

# pip install torch torchvision
echo $CONDA_PREFIX >> begin_test.log
echo "Starting integration test" >> begin_test.log
pip uninstall awsio -y
pip install $S3_WHL --no-deps
echo "Installed $S3_WHL" >> begin_test.log
echo $(python --version) >> begin_test.log
aws s3 cp begin_test.log $LOG_DIR

cd aws_io/benchmarks/cv/mmclassification
echo $PWD >> integ_test.log
python -m torch.distributed.launch \
        --nproc_per_node=$NUM_GPU --nnodes=1 --node_rank=0 \
        tools/train.py configs/imagenets3/resnet50_b64.py \
        --work-dir=work_dir/junk_change3 \
        --launcher="pytorch" \
        --no-validate | tee integ_test.log
aws s3 cp integ_test.log $LOG_DIR
