#!/bin/bash
# Start distributed training using SLURM, upload the metrics to cloudwatch
MODEL=$1
NUM_GPUS_PER_NODE=$2
gpu_num=$3
CONFIG_FILE=$4
WORK_DIR=$5
EPOCH_NUM=$6
LEARNING_RATE=$7

RETRY_ATTEMPTS=3

#Runs train script through an srun job
COMMAND="srun --job-name=${MODEL}
    --gres=gpu:${NUM_GPUS_PER_NODE}
    --ntasks=${gpu_num}
    --ntasks-per-node=${NUM_GPUS_PER_NODE}
    --kill-on-bad-exit=1
    python -u tools/train.py
    ${CONFIG_FILE} --work-dir=${WORK_DIR} --launcher='slurm' --learning_rate=${LEARNING_RATE}"


SECONDS=0
echo
eval $COMMAND

#Retry if job fails
n=0
until [ "$n" -ge $RETRY_ATTEMPTS ]
do
   SECONDS=0
   eval $COMMAND && break  # Will break when
   echo "Failed to train ${MODEL} with Slurm, attemp no ${n}"
   n=$((n+1))
   sleep 15
done

end_time=$SECONDS
TIME=$((end_time))
num_nodes=$(( gpu_num / NUM_GPUS_PER_NODE))


echo $TIME > "${WORK_DIR}/timetaken"
echo ""
echo "Training Complete. Took ${TIME} seconds"
echo "Pushing metrics to CloudWatch"

#calling python script to push metrics to cloudwatch
#make sure to configure aws-cli prior
DIRECTORY=`dirname $0`
COMMAND="python ${DIRECTORY}/send_metrics.py ${gpu_num} ${WORK_DIR} ${MODEL}_${num_nodes}_nodes ${EPOCH_NUM} ${CONFIG_FILE}"
eval $COMMAND
if [ $? -eq 0 ]; then
  echo "Successfully pushed metrics to cloudwatch "
else
  echo "Failed to push metrics to cloudwatch "
fi
echo ""
echo "Cleaning work directory.."
# TODO : Uncomment this when everything is finalized
#rm -rf ${WORK_DIR}

echo "Job Successful"
