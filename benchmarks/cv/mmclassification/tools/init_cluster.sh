#!/bin/bash
# This script creates data,code and config setup on every node in the cluster
mkdir ~/.aws

echo "Copying aws and ssh credentials to home"
cp /fsx/.aws/* ~/.aws
cp /fsx/.ssh/* ~/.ssh

echo ""
echo "Pulling latest from PyTorchBenchmarks..."
cd /fsx/PyTorchBenchmarks/
git checkout master
git pull

cd /fsx/PyTorchBenchmarks/benchmarks/mmclassification
ln -sfn /fsx/data /fsx/PyTorchBenchmarks/benchmarks/mmclassification

/fsx/.conda/bin/conda init
source ~/.bashrc
source /fsx/.conda/etc/profile.d/conda.sh
conda activate open-mmlab;

echo "Finished init_cluster.sh"
