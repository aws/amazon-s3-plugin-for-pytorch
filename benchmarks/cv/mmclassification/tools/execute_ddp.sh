#!/bin/bash
# This script runs the input model using pytorch ddp library

MODEL=$1
NUM_GPUS_PER_NODE=$2
NUM_NODES=$3
CONFIG_FILE=$4
WORK_DIR=$5
EPOCH_NUM=$6
FP16=$7
MASTER_IP=$8
RANK=$9

#Runs train script through the torch distributed module
NUM_GPUS=$(( NUM_GPUS_PER_NODE * NUM_NODES ))

source /mnt/fsx/.conda/etc/profile.d/conda.sh
conda activate open-mmlab

COMMAND="\
    python -m torch.distributed.launch \
      --nproc_per_node=$NUM_GPUS_PER_NODE \
      --nnodes=${NUM_NODES} \
      --node_rank=$RANK \
      --master_addr=${MASTER_IP} \
      --master_port=1234 \
    tools/train.py ${CONFIG_FILE} \
      --work-dir=${WORK_DIR} \
      --launcher="pytorch""

SECONDS=0
eval "${COMMAND}"

# Retry if job fails
WAIT_BEFORE_RETRY=2
# TODO: Limit retry attempts
while [ $? -ne 0 ]; do
    sleep $WAIT_BEFORE_RETRY
    SECONDS=0
    eval $COMMAND
done
end_time=$SECONDS
TIME=$((end_time))

if [ $RANK -eq 0 ]; then
	echo $TIME > "${WORK_DIR}/timetaken"
	echo ""
	echo "Training Complete. Took ${TIME} seconds"
	echo "Pushing metrics to CloudWatch"

	#Calling python script to push metrics to cloudwatch
	#make sure to configure aws-cli prior
	DIRECTORY=`dirname $0`
	COMMAND="python ${DIRECTORY}/send_metrics.py ${NUM_GPUS} ${WORK_DIR} ${MODEL} ${EPOCH_NUM}"
	eval $COMMAND

  if [ $? -ne 0 ]; then
      echo "Failed to send_metrics "
      exit
  fi
	echo ""
	echo "Metrics pushed. Cleaning work directory.."
	#rm -rf ${WORK_DIR}

	echo "Job Successful"
fi
