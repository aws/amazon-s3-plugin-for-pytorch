# File Structure

1. run_benchmark_job.sh: For given models, creates multinode cluster and trains specific model using distributed slurm.  
    - init_cluster.sh: Sets up environment, pulls latest code in cluster created from run_benchmark_job.sh
    - run_train.sh: Parses input arguments and calculates learning rate based on number of nodes.
        - execute_srun.sh: Train distributed model using slurm.
            - train.py: Python training script to train model.
            - send_metrics.py: Upload metrics to cloudwatch.   
2. execute_ddp.sh: Train model using PyTorch DDP. Note: This is not integrated in benchmarking framework yet. Can be used  
to train model using PyTorch DDP. 
 
