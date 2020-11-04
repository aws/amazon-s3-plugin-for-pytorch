import argparse
import boto3
import os
import random
import subprocess

from datetime import datetime
from parse import *

BERT_CONFIG='bert_config.json'
RESULTS_DIR='./results/'
CHECKPOINTS_DIR='./results/checkpoints/'
TRAIN_SCRIPT='/home/ubuntu/Pytorch-NLP/BERT/NV/run_pretraining.py'

def get_commands(args):
    num_nodes=args.num_nodes
    if num_nodes > 1:
        node_rank=args.node_rank
        master_addr=args.master_addr
        master_port=args.master_port

    # phase 1
    train_batch_size=args.train_batch_size // num_nodes
    learning_rate=args.learning_rate
    precision=args.precision
    num_gpus=args.num_gpus
    warmup_proportion=args.warmup_proportion
    train_steps=args.train_steps
    save_checkpoint_steps=args.save_checkpoint_steps
    resume_training=args.resume_training
    accumulate_gradients=args.accumulate_gradients
    gradient_accumulation_steps=args.gradient_accumulation_steps
    seed=random.randint(0, 32767)
    allreduce_post_accumulation=args.allreduce_post_accumulation
    allreduce_post_accumulation_fp16=args.allreduce_post_accumulation_fp16
    accumulate_into_fp16=args.accumulate_into_fp16

    # phase 2
    train_batch_size_phase2=args.train_batch_size_phase2 // num_nodes
    learning_rate_phase2=args.learning_rate_phase2
    warmup_proportion_phase2=args.warmup_proportion_phase2
    train_steps_phase2=args.train_steps_phase2
    gradient_accumulation_steps_phase2=args.gradient_accumulation_steps_phase2 // num_nodes

    subprocess.run(f"mkdir -p {CHECKPOINTS_DIR}", shell=True)

    # start creating command for phase 1
    DATA_DIR='/home/ubuntu/NV-data/phase1/training/'

    PREC = ''
    if precision == 'fp16':
        PREC='--fp16'
    elif precision =='fp32':
        PREC='--fp32'
    else:
        raise Exception(f"Unknown precision argument {precision}")

    ACCUMULATE_GRADIENTS=""
    if accumulate_gradients:
        ACCUMULATE_GRADIENTS=f"--gradient_accumulation_steps={gradient_accumulation_steps}"

    CHECKPOINT=""
    if resume_training:
       CHECKPOINT="--resume_from_checkpoint"

    ALL_REDUCE_POST_ACCUMULATION=""
    if allreduce_post_accumulation:
       ALL_REDUCE_POST_ACCUMULATION="--allreduce_post_accumulation"

    ALL_REDUCE_POST_ACCUMULATION_FP16=""
    if allreduce_post_accumulation_fp16:
        ALL_REDUCE_POST_ACCUMULATION_FP16="--allreduce_post_accumulation_fp16"
        
    ACCUMULATE_INTO_FP16=""
    if accumulate_into_fp16:
        ACCUMULATE_INTO_FP16="--accumulate_into_fp16"

    CMD = f" {TRAIN_SCRIPT}" \
        f" --input_dir={DATA_DIR}" \
        f" --output_dir={CHECKPOINTS_DIR}" \
        f" --config_file={BERT_CONFIG}" \
        f" --bert_model=bert-large-uncased" \
        f" --train_batch_size={train_batch_size}" \
        f" --max_seq_length=128" \
        f" --max_predictions_per_seq=20" \
        f" --max_steps={train_steps}" \
        f" --warmup_proportion={warmup_proportion}" \
        f" --num_steps_per_checkpoint={save_checkpoint_steps}" \
        f" --learning_rate={learning_rate}" \
        f" --seed={seed}" \
        f" {PREC}" \
        f" {ACCUMULATE_GRADIENTS}" \
        f" {CHECKPOINT}" \
        f" {ALL_REDUCE_POST_ACCUMULATION}" \
        f" {ALL_REDUCE_POST_ACCUMULATION_FP16}" \
        f" {ACCUMULATE_INTO_FP16}" \
        f" --do_train"

    if num_nodes > 1:
        cmd_p1=f"python3 -m torch.distributed.launch --nproc_per_node={num_gpus} --nnodes={num_nodes} --node_rank={node_rank} --master_addr={master_addr} --master_port={master_port} {CMD}"
    elif num_gpus > 1:
        cmd_p1=f"python3 -m torch.distributed.launch --nproc_per_node={num_gpus} {CMD}"
    else:
        cmd_p1=f"python3 {CMD}"

    ACCUMULATE_GRADIENTS=""
    if accumulate_gradients:
        ACCUMULATE_GRADIENTS=f"--gradient_accumulation_steps={gradient_accumulation_steps_phase2}"

    # start creating command for phase 2
    DATA_DIR='/home/ubuntu/NV-data/phase2/training/'
    CMD = f" {TRAIN_SCRIPT}" \
        f" --input_dir={DATA_DIR}" \
        f" --output_dir={CHECKPOINTS_DIR}" \
        f" --config_file={BERT_CONFIG}" \
        f" --bert_model=bert-large-uncased" \
        f" --train_batch_size={train_batch_size_phase2}" \
        f" --max_seq_length=512" \
        f" --max_predictions_per_seq=80" \
        f" --max_steps={train_steps_phase2}" \
        f" --warmup_proportion={warmup_proportion_phase2}" \
        f" --num_steps_per_checkpoint={save_checkpoint_steps}" \
        f" --learning_rate={learning_rate_phase2}" \
        f" --seed={seed}" \
        f" {PREC}" \
        f" {ACCUMULATE_GRADIENTS}" \
        f" {CHECKPOINT}" \
        f" {ALL_REDUCE_POST_ACCUMULATION}" \
        f" {ALL_REDUCE_POST_ACCUMULATION_FP16}" \
        f" {ACCUMULATE_INTO_FP16}" \
        f" --do_train --phase2 --resume_from_checkpoint --phase1_end_step={train_steps}"

    if num_nodes > 1:
        cmd_p2=f"python3 -m torch.distributed.launch --nproc_per_node={num_gpus} --nnodes={num_nodes} --node_rank={node_rank} --master_addr={master_addr} --master_port={master_port} {CMD}"
    elif num_gpus > 1:
        cmd_p2=f"python3 -m torch.distributed.launch --nproc_per_node={num_gpus} {CMD}"
    else:
        cmd_p2=f"python3  {CMD}"

    return cmd_p1, cmd_p2

def get_metrics(phase, log_file, num_gpus, batch_size):
    # retrieve metrics
    f = open(log_file, "r")
    lines = f.readlines()

    # throughput
    str_format = "{},  {}it/s{}"
    throughput = [line for line in lines if "Iteration" in line][-1]
    _,throughput, _ = parse(str_format, throughput)
    throughput = float(throughput)

    # average loss
    loss = [line for line in lines if "Average Loss" in line][-1]
    str_format = "Step:{} Average Loss = {} Step Loss = {} LR {}"
    _, loss, _, _ = parse(str_format, loss)
    loss = float(loss)

    # total steps and final loss
    final_num = [line.strip() for line in lines if "Total Steps" in line][0]
    str_format = "{} Total Steps:{} Final Loss = {}"
    _, total_steps, final_loss = parse(str_format, final_num)
    total_steps = int(total_steps)
    final_loss = float(final_loss)

    # total train time
    train_time = [line.strip() for line in lines if "Total time taken" in line][0]
    str_format = "Total time taken {}"
    train_time = float(parse(str_format, train_time)[0])

    train_perf = throughput * num_gpus * batch_size
    print(f"training throughput: {train_perf} sequences/second")
    print(f"average loss: {loss}")
    print(f"final loss: {final_loss}")

    results = {}
    results[f"throughput_p{phase}"] = train_perf
    results[f"loss_p{phase}"] = loss
    results[f"total_steps_p{phase}"] = total_steps
    results[f"final_loss_p{phase}"] = final_loss
    results[f"train_time_p{phase}"] = train_time

    return results

def main(args):
    cmd_p1, cmd_p2 = get_commands(args)
    s3 = boto3.resource('s3')

    # get names for log files
    timestamp=datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")
    log_file_p1=f"{RESULTS_DIR}/{timestamp}/pytorch_bert_pretraining_phase_1.log"
    log_file_p2=f"{RESULTS_DIR}/{timestamp}/pytorch_bert_pretraining_phase_2.log"

    # run pretraining phase 1
    print(f"Logs written to {log_file_p1}")
    subprocess.run(f"{cmd_p1}  |& tee {log_file_p1}", shell=True)
    if args.node_rank == 0:
        batch_size = args.train_batch_size // args.num_nodes
        results_p1 = get_metrics(1, log_file_p1, args.num_gpus, batch_size)
        print("Finished pretraining phase 1.")

        # upload log file and checkpoints to s3
        s3.meta.client.upload_file(log_file_p1, 'choidong-dev', f"{timestamp}/{log_file_p1}")
        # upload checkpoints
        for checkpoint in os.listdir(CHECKPOINTS_DIR):
            s3.meta.client.upload_file(checkpoint, 'choidong-dev', f"{timestamp}/checkpoints/{checkpoint}")
    # kill python processes
    subprocess.run("pkill -9 python", shell=True)

    if args.node_rank != 0:
        # TODO copy checkpiont to all nodes
        pass

    # run pretraining phase 2
    print(f"Logs written to {log_file_p2}")
    subprocess.run(f"{cmd_p2}  |& tee {log_file_p1}", shell=True)
    if args.node_rank == 0:
        batch_size = args.train_batch_size_phase2 // args.num_nodes
        results_p2 = get_metrics(2, log_file_p2, args.num_gpus, batch_size)

    print("Finished pretraining phase 2.")

    # TODO send metrics (results_p1, results_p2)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    # distributed training parameters
    parser.add_argument('--num_nodes', type=int, default=1)
    parser.add_argument('--node_rank', type=int, default=0)
    parser.add_argument('--master_addr', type=str, default="172.31.1.0")
    parser.add_argument('--master_port', type=int, default='8888')

    # specify phase 1 params
    parser.add_argument('--train_batch_size', type=int, default=8192)
    parser.add_argument('--learning_rate', type=float, default=6e-3)
    parser.add_argument('--precision', type=str, default='fp16')
    parser.add_argument('--num_gpus', type=int, default=8)
    parser.add_argument('--warmup_proportion', type=float, default=0.2843)
    parser.add_argument('--train_steps', type=int, default=7038)
    parser.add_argument('--save_checkpoint_steps', type=int, default=200)
    parser.add_argument('--resume_training', type=bool, default=False)
    parser.add_argument('--accumulate_gradients', type=bool, default=True)
    parser.add_argument('--gradient_accumulation_steps', type=int, default=128)
    parser.add_argument('--allreduce_post_accumulation', type=bool, default=True)
    parser.add_argument('--allreduce_post_accumulation_fp16', type=bool, default=True)
    parser.add_argument('--accumulate_into_fp16', type=bool, default=False)

    # specify phase 2 params
    parser.add_argument('--train_batch_size_phase2', type=int, default=4096)
    parser.add_argument('--learning_rate_phase2', type=float, default=4e-3)
    parser.add_argument('--warmup_proportion_phase2', type=float, default=0.128)
    parser.add_argument('--train_steps_phase2', type=int, default=1563)
    parser.add_argument('--gradient_accumulation_steps_phase2', type=int, default=512)

    args = parser.parse_args()

    main(args)
