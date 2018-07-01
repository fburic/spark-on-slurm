if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    echo "[ERROR] Don't run $0, source it. Run:  source $0" >&2
    exit 1
fi

module load Spark

####################################
## Set parameters for Spark cluster
####################################
export SPARK_HOME=$(dirname $(dirname $(which spark-config.sh)))
export SPARK_LOCAL_DIRS=$HOME/spark/tmp
export SPARK_NO_DAEMONIZE=true
export SPARK_LOG_DIR=$SPARK_LOCAL_DIRS

mkdir -p $SPARK_LOCAL_DIRS


####################################
## Set Spark master
####################################

export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
export MAX_SLAVES=$SLURM_JOB_NUM_NODES

# Ask to start the Master node on its own cluster node
# Requires #SBATCH --distribution cyclic
srun --nodes 1 --ntasks 1 start-master.sh &


# May take some time for the Master node to start
# Keep trying to read its hostname from the logfile
while true; do
	export MASTER_HOST=$(egrep -o "Master --host .* " spark_cluster.err | awk '{print $3}')

	if [[ ! -z $MASTER_HOST ]]; then
		break
	fi

	sleep 1
done

export MASTER_URL="spark://${MASTER_HOST}:${SPARK_MASTER_PORT}"


####################################
## Set Spark workers
####################################

export SPARK_WORKER_DIR=$SPARK_LOCAL_DIRS
export SPARK_WORKER_CORES=$SLURM_CPUS_ON_NODE

# Ask to start the Master node on its own cluster node
# Requires #SBATCH --distribution cyclic
#
# Use spark defaults for worker resources (all mem -1 GB, all cores)
# since using exclusive for starting spark worker
srun --nodes 1 --ntasks 1 start-slave.sh ${MASTER_URL} &  > spark_slave.log 2> spark_slave.err


####################################
## Write out some info
####################################

date "+%Y-%m-%d-%H:%M:%S" > spark.info
echo "Spark master node: ${MASTER_HOST}" >> spark.info
echo "==== ssh tunnels ====" >> spark.info
echo "SparkUI:  ssh -N -L ${SPARK_MASTER_WEBUI_PORT}:${MASTER_HOST}:${SPARK_MASTER_WEBUI_PORT} ${USER}@$(hostname -s)" >> spark.info
echo "SparkUI:  ssh -L localhost:5050:localhost:4040 $(hostname -s) 'ssh -N -L localhost:4040:localhost:4040 ${MASTER_HOST}'" >> spark.info
