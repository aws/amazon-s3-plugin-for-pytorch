. ~/.bashrc;. /home/ubuntu/.conda/etc/profile.d/conda.sh ;
conda activate pytorch;


for PTBRANCH in 'mansimane_dev_dq_3'
do
  cd /home/ubuntu/pytorch;
  git checkout $PTBRANCH;
  cd /home/ubuntu/aws_io/benchmarks/cv/mmclassification;

  for LATENCY in '0ms', '5ms'
  do
    for N_WORKER in {8,}
    do
      for PREFETCH_FACTOR in {2,}
      do
        for TRIAL in {0,}
        do
          SECONDS=0
          EPOCH_NUM=1
          WORK_DIR=work_dirs/${LATENCY}_trial_${TRIAL}_n_worker_${N_WORKER}_prefetch_${PREFETCH_FACTOR}_${PTBRANCH}_cache_flush
          METRICS_CSV=work_dirs_csv/${LATENCY}_n_worker_${N_WORKER}_prefetch_${PREFETCH_FACTOR}_${PTBRANCH}_cache_flush.csv
          # Clear the cache
          sudo sysctl -w vm.drop_caches=3

          /home/ubuntu/.conda/envs/pytorch/bin/python -u -m torch.distributed.launch \
          --nproc_per_node=8 --master_port=29500 /home/ubuntu/aws_io/benchmarks/cv/mmclassification/tools/train.py \
          configs/imagenet/resnet50_io_latency_${LATENCY}.py \
          --work-dir=$WORK_DIR \
          --n_workers=$N_WORKER \
          --n_epochs=$EPOCH_NUM \
          --prefetch_factor=$PREFETCH_FACTOR \
          --launcher pytorch

          end_time=$SECONDS
          TIME=$((end_time))
          echo $TIME > "${WORK_DIR}/timetaken"

          python tools/send_metrics.py 8 ${WORK_DIR} resnet50 ${EPOCH_NUM} \
          mmclassification/configs/imagenet/resnet50_io_latency.py --metrics_csv_file=$METRICS_CSV> ${WORK_DIR}/metrics
        done
        python tools/analyze_variability.py --metrics_csv_file=$METRICS_CSV > work_dirs_var/${LATENCY}_n_worker_${N_WORKER}_prefetch_${PREFETCH_FACTOR}_${PTBRANCH}_cache_flush

      done
    done
  done
done
