#!/bin/bash

# Specify dist training params
NUM_NODES=4
NODE_RANK=1
MASTER_ADDR="172.31.28.254"
MASTER_PORT="1234"

# Specify phase 1 params
train_batch_size=${1:-2048}
learning_rate=${2:-"6e-3"}
precision=${3:-"fp16"}
num_gpus=${4:-8}
warmup_proportion=${5:-"0.2843"}
train_steps=${6:-7038}
save_checkpoint_steps=${7:-200}
resume_training=${8:-"false"}
create_logfile=${9:-"true"}
accumulate_gradients=${10:-"true"}
gradient_accumulation_steps=${11:-128}
seed=${12:-$RANDOM}
job_name=${13:-"bert_lamb_pretraining"}
allreduce_post_accumulation=${14:-"true"}
allreduce_post_accumulation_fp16=${15:-"true"}
accumulate_into_fp16=${16:-"false"}


# Specify phase 2 params
train_batch_size_phase2=${1:-1024}
learning_rate_phase2=${2:-"4e-3"}
warmup_proportion_phase2=${5:-"0.128"}
train_steps_phase2=${6:-1563}
gradient_accumulation_steps_phase2=${11:-256}

# Specify phase 1 data path
DATA_DIR=/home/ubuntu/NV-data/phase1/training/
BERT_CONFIG=bert_config.json
RESULTS_DIR=/home/ubuntu/aws_io/benchmarks/bert/results
CHECKPOINTS_DIR=/home/ubuntu/aws_io/benchmarks/bert/checkpoints


mkdir -p $CHECKPOINTS_DIR


if [ ! -d "$CHECKPOINTS_DIR" ] ; then
   echo "Warning! $CHECKPOINTS_DIR directory missing."
   echo "Checkpoints will be written to $RESULTS_DIR instead."
   CHECKPOINTS_DIR=$RESULTS_DIR
fi
if [ ! -f "$BERT_CONFIG" ] ; then
   echo "Error! BERT large configuration file not found at $BERT_CONFIG"
   exit -1
fi

PREC=""
if [ "$precision" = "fp16" ] ; then
   PREC="--fp16"
elif [ "$precision" = "fp32" ] ; then
   PREC=""
else
   echo "Unknown <precision> argument"
   exit -2
fi

ACCUMULATE_GRADIENTS=""
if [ "$accumulate_gradients" == "true" ] ; then
   ACCUMULATE_GRADIENTS="--gradient_accumulation_steps=$gradient_accumulation_steps"
fi

CHECKPOINT=""
if [ "$resume_training" == "true" ] ; then
   CHECKPOINT="--resume_from_checkpoint"
fi

ALL_REDUCE_POST_ACCUMULATION=""
if [ "$allreduce_post_accumulation" == "true" ] ; then
   ALL_REDUCE_POST_ACCUMULATION="--allreduce_post_accumulation"
fi

ALL_REDUCE_POST_ACCUMULATION_FP16=""
if [ "$allreduce_post_accumulation_fp16" == "true" ] ; then
   ALL_REDUCE_POST_ACCUMULATION_FP16="--allreduce_post_accumulation_fp16"
fi

ACCUMULATE_INTO_FP16=""
if [ "$accumulate_into_fp16" == "true" ] ; then
   ACCUMULATE_INTO_FP16="--accumulate_into_fp16"
fi

echo $DATA_DIR
INPUT_DIR=$DATA_DIR
CMD=" /home/ubuntu/aws_io/benchmarks/bert/run_pretraining.py"
CMD+=" --input_dir=$DATA_DIR"
CMD+=" --output_dir=$CHECKPOINTS_DIR"
CMD+=" --config_file=$BERT_CONFIG"
CMD+=" --bert_model=bert-large-uncased"
CMD+=" --train_batch_size=$train_batch_size"
CMD+=" --max_seq_length=128"
CMD+=" --max_predictions_per_seq=20"
CMD+=" --max_steps=$train_steps"
CMD+=" --warmup_proportion=$warmup_proportion"
CMD+=" --num_steps_per_checkpoint=$save_checkpoint_steps"
CMD+=" --learning_rate=$learning_rate"
CMD+=" --seed=$seed"
CMD+=" $PREC"
CMD+=" $ACCUMULATE_GRADIENTS"
CMD+=" $CHECKPOINT"
CMD+=" $ALL_REDUCE_POST_ACCUMULATION"
CMD+=" $ALL_REDUCE_POST_ACCUMULATION_FP16"
CMD+=" $ACCUMULATE_INTO_FP16"
CMD+=" --do_train"


CMD="python3 -m torch.distributed.launch --nproc_per_node=$num_gpus --nnodes=$NUM_NODES --node_rank=$NODE_RANK --master_addr=$MASTER_ADDR --master_port=$MASTER_PORT $CMD"



if [ "$create_logfile" = "true" ] ; then
  export GBS=$(expr $train_batch_size \* $num_gpus)
  printf -v TAG "pyt_bert_pretraining_phase1_%s_gbs%d" "$precision" $GBS
  DATESTAMP=`date +'%y%m%d%H%M%S'`
  LOGFILE=$RESULTS_DIR/$job_name.$TAG.$DATESTAMP.log
  printf "Logs written to %s\n" "$LOGFILE"
fi

set -x
if [ -z "$LOGFILE" ] ; then
   $CMD
else
   (
     $CMD
   ) |& tee $LOGFILE
fi

set +x

echo "finished pretraining, starting benchmarking"

target_loss=15
THROUGHPUT=10
THRESHOLD=0.9

throughput=`cat $LOGFILE | grep Iteration | tail -1 | awk -F'it/s' '{print $1}' | awk -F',' '{print $2}' | egrep -o [0-9.]+`
loss=`cat $LOGFILE | grep 'Average Loss' | tail -1 | awk -F'Average Loss =' '{print $2}' | awk -F' ' '{print $1}' | egrep -o [0-9.]+`
final_loss=`cat $LOGFILE | grep 'Total Steps' | tail -1 | awk -F'Final Loss =' '{print $2}' | awk -F' ' '{print $1}' | egrep -o [0-9.]+`

train_perf=$(awk 'BEGIN {print ('$throughput' * '$num_gpus' * '$train_batch_size')}')
echo " training throughput phase1: $train_perf sequences/second"
echo "average loss: $loss"
echo "final loss: $final_loss"

#Start Phase2

# Specify phase 2 data path
#DATA_DIR=/home/ubuntu/NV-data/phase2/training/
#
#PREC=""
#if [ "$precision" = "fp16" ] ; then
#   PREC="--fp16"
#elif [ "$precision" = "fp32" ] ; then
#   PREC=""
#else
#   echo "Unknown <precision> argument"
#   exit -2
#fi
#
#ACCUMULATE_GRADIENTS=""
#if [ "$accumulate_gradients" == "true" ] ; then
#   ACCUMULATE_GRADIENTS="--gradient_accumulation_steps=$gradient_accumulation_steps_phase2"
#fi
#
#ALL_REDUCE_POST_ACCUMULATION=""
#if [ "$allreduce_post_accumulation" == "true" ] ; then
#   ALL_REDUCE_POST_ACCUMULATION="--allreduce_post_accumulation"
#fi
#
#ALL_REDUCE_POST_ACCUMULATION_FP16=""
#if [ "$allreduce_post_accumulation_fp16" == "true" ] ; then
#   ALL_REDUCE_POST_ACCUMULATION_FP16="--allreduce_post_accumulation_fp16"
#fi
#
#ACCUMULATE_INTO_FP16=""
#if [ "$accumulate_into_fp16" == "true" ] ; then
#   ACCUMULATE_INTO_FP16="--accumulate_into_fp16"
#fi
#
#echo $DATA_DIR
#INPUT_DIR=$DATA_DIR
#CMD=" /home/ubuntu/Pytorch-NLP/BERT/NV/run_pretraining.py"
#CMD+=" --input_dir=$DATA_DIR"
#CMD+=" --output_dir=$CHECKPOINTS_DIR"
#CMD+=" --config_file=$BERT_CONFIG"
#CMD+=" --bert_model=bert-large-uncased"
#CMD+=" --train_batch_size=$train_batch_size_phase2"
#CMD+=" --max_seq_length=512"
#CMD+=" --max_predictions_per_seq=80"
#CMD+=" --max_steps=$train_steps_phase2"
#CMD+=" --warmup_proportion=$warmup_proportion_phase2"
#CMD+=" --num_steps_per_checkpoint=$save_checkpoint_steps"
#CMD+=" --learning_rate=$learning_rate_phase2"
#CMD+=" --seed=$seed"
#CMD+=" $PREC"
#CMD+=" $ACCUMULATE_GRADIENTS"
#CMD+=" $CHECKPOINT"
#CMD+=" $ALL_REDUCE_POST_ACCUMULATION"
#CMD+=" $ALL_REDUCE_POST_ACCUMULATION_FP16"
#CMD+=" $ACCUMULATE_INTO_FP16"
#CMD+=" --do_train --phase2 --resume_from_checkpoint --phase1_end_step=$train_steps"
#
#
#CMD="python3 -m torch.distributed.launch --nproc_per_node=$num_gpus --nnodes=$NUM_NODES --node_rank=$NODE_RANK --master_addr=$MASTER_ADDR --master_port=$MASTER_PORT $CMD"
#
#
#if [ "$create_logfile" = "true" ] ; then
#  export GBS=$(expr $train_batch_size_phase2 \* $num_gpus)
#  printf -v TAG "pyt_bert_pretraining_phase2_%s_gbs%d" "$precision" $GBS
#  DATESTAMP=`date +'%y%m%d%H%M%S'`
#  LOGFILE=$RESULTS_DIR/$job_name.$TAG.$DATESTAMP.log
#  printf "Logs written to %s\n" "$LOGFILE"
#fi
#
#set -x
#if [ -z "$LOGFILE" ] ; then
#   $CMD
#else
#   (
#     $CMD
#   ) |& tee $LOGFILE
#fi
#
#set +x
#
#echo "finished phase2"
#throughput=`cat $LOGFILE | grep Iteration | tail -1 | awk -F'it/s' '{print $1}' | awk -F',' '{print $2}' | egrep -o [0-9.]+`
#loss=`cat $LOGFILE | grep 'Average Loss' | tail -1 | awk -F'Average Loss =' '{print $2}' | awk -F' ' '{print $1}' | egrep -o [0-9.]+`
#final_loss=`cat $LOGFILE | grep 'Total Steps' | tail -1 | awk -F'Final Loss =' '{print $2}' | awk -F' ' '{print $1}' | egrep -o [0-9.]+`
#
#train_perf=$(awk 'BEGIN {print ('$throughput' * '$num_gpus' * '$train_batch_size_phase2')}')
#echo " training throughput phase2: $train_perf sequences/second"
#echo "average loss: $loss"
#echo "final loss: $final_loss"

