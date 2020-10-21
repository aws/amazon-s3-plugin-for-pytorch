#!/bin/bash
set -e

function cleanup {
  echo "Cluster creation failed. Job incompolete."
  echo "Cleaning cluster.."
  pcluster delete mmclscluster
}

trap cleanup EXIT
trap cleanup SIGINT

source ~/miniconda3/etc/profile.d/conda.sh

conda activate pcluster_client

set -x

num_gpus_per_node=8
NUM_EPOCHS=100
for num_nodes in {1,2,4,8}
do
  sed -i "38s/.*/min_count = ${num_nodes}/" ~/.parallelcluster/config
  sed -i "39s/.*/max_count = ${num_nodes}/" ~/.parallelcluster/config
  timeout 2400 pcluster create -c ~/.parallelcluster/config mmclscluster
  if [ $? -ne 0 ]; then
      echo "Could not create cluster"
      exit
  fi
  echo "Cluster created successfully."
  echo "Starting training job on ${num_nodes} nodes"
  for model_name in {'resnext101_32x8d_ec2','resnet50_ec2'}
  do
    echo ""
    echo "Starting training on ${model_name} with ${num_nodes} nodes"
    num_gpus=$(( num_nodes * $num_gpus_per_node))
    backbone="${model_name}.py"
    cmd='. ~/.bashrc;. /fsx/.conda/etc/profile.d/conda.sh ;
          conda activate open-mmlab;
          cd /fsx/PyTorchBenchmarks/benchmarks/mmclassification;
          export PATH=$PATH:/opt/slurm/bin/;
          chmod +x tools/init_cluster.sh;
          ./tools/init_cluster.sh;
          chmod +x tools/run_train.sh;
          conda activate open-mmlab;
          ./tools/run_train.sh -m '"${model_name}"' -c '"${backbone}"' -g '"${num_gpus}"' -n '"${NUM_EPOCHS}"'  -o '"${num_nodes}"';'
    pcluster ssh mmclscluster -i ~/.ssh/mmcls.pem $cmd
    echo ""
    echo ""
    echo "Training complete on ${model_name} with ${num_nodes} nodes"
  done
  pcluster delete mmclscluster
done
echo ""
echo "All training has been complete on 1,2,4,8 nodes. Terminating node cluster now."

