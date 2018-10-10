#!/usr/bin/env bash

#SBATCH -t 4:00:00
#SBATCH --nodes 2
#SBATCH --ntasks-per-node 1
#SBATCH --cpus-per-task 20
#SBATCH --distribution cyclic
#SBATCH --output=spark_cluster.log
#SBATCH --error=spark_cluster.err

set -e

## LOAD RELEVANT MODULES HERE #########
module load Anaconda3
#######################################

source start_spark.sh

export PYSPARK_PYTHON=$(which python)
export PYSPARK_DRIVER_PYTHON=$(which jupyter)
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser --port=8889 --ip=127.0.0.1"

echo "Jupyter:  ssh -f -L localhost:8889:localhost:8889 $(hostname -s) 'ssh -N -L localhost:8889:localhost:8889 ${MASTER_HOST}' " >> spark.info

N_EXECUTOR_CORES=$(expr $SLURM_JOB_NUM_NODES \* $SLURM_NTASKS_PER_NODE \* $SLURM_CPUS_PER_TASK)
EXECUTOR_MEMORY=$(expr $SLURM_MEM_PER_NODE / $SLURM_CPUS_ON_NODE)

pyspark \
	--total-executor-cores $N_EXECUTOR_CORES \
	--executor-memory ${EXECUTOR_MEMORY}M \
	--driver-memory 3G \
	--master ${MASTER_URL} \

sleep infinity
