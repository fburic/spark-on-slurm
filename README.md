# Spark on Slurm

Collection of scripts to run Spark on a cluster with
resources managed by [Slurm](https://slurm.schedmd.com/).

Scripts were developed with Spark `2.2.0` and Slurm `17.11.5`

The scripts assume there is a Spark
([Lmod](https://lmod.readthedocs.io/en/latest/010_user.html))
module available for Slurm.


## Requirements

* Slurm with a Spark module
* pyspark (best installed through conda)
* jupyter (best installed through conda)


## Usage

Submit the script matching your use case to Slurm.

### Interactive Jupyter session

The script `start_spark_for_jupyter.sh` will start an interactive Jupyter
session connected to a Spark context on the cluster.

#### Steps

0. Edit `start_spark_for_jupyter.sh` to add any relevant Slurm modules 
   and activate the needed conda environment.

1. Submit it to Slurm as:

  `sbatch -A <account> -p <slurm-partition> start_spark_for_jupyter.sh`

2. Wait for the job to start running

3. `cat spark.info` to get SSH tunneling commands

#### Behaviour

The script does the following:

1. starts Spark on the cluster (see section below for more details)
2. registers Jupyter as the frontend for pyspark
3. starts a pyspark session against the master node started in step 1
4. write commands for setting up SSH tunnels to `spark.info`

In the Jupyter environment there will be a (pre-)defined Spark context
(called `spark`) available.

The script will ask for resources exhaustively:

- all allocated memory for executors and driver
- all allocated CPU cores for `--total-executor-cores`

This can however be problematic for some libraries that allocate
memory outside the JVM heap, as is the case with Threads.
Try lowering executor memory values if you get errors like:

*spark java.lang.OutOfMemoryError: unable to create new native thread*

See point 4 in this post:
https://dzone.com/articles/troubleshoot-outofmemoryerror-unable-to-create-new


### Spark initialization on the cluster (core script)

The script `start_spark.sh` simply starts a Spark session in cluster mode.
The other scripts are wrappers for various use-cases.
Submit it as:

`sbatch -A <account> -p <slurm-partition> start_spark.sh`

Upon success, information about SSH tunnels is written to `spark.info` in
the directory from which the job was submitted.
Note that the SparkUI for each application (at port 4040) will only be
available after the application is started (by loading a Jupyter notebook).

Log information is written to `spark_cluster.err`.

Further interaction with the session is left to the user.

The script will try to start the spark master and worker nodes
on different cluster machines. This is done by setting
`#SLURM --distribution cyclic` and making a separate `srun --nodes 1 --ntasks 1`
call to the the master and worker startup scripts.
See the following for more information about this:
* Slurm [documentation](https://slurm.schedmd.com/srun.html#lbAE)
* Discussion about the pattern: https://bugs.schedmd.com/show_bug.cgi?id=3382



## Known issues

Both Spark and Jupyter write to the same log file `spark_cluster.err` since
stdout output is redirected to the sbatch logfile


## Tests

Tests are provided in the `test` directory to check that Spark is running
properly.


## References

* More detailed info about how Slurm manages resources: https://slurm.schedmd.com/mc_support.html

*  Starting points for these scripts:
   * Princeton University https://researchcomputing.princeton.edu/faq/spark-via-slurm
   * University of Helsinki https://wiki.helsinki.fi/display/it4sci/Spark+User+Guide

* Rather technical gitbook about Spark internals:
https://legacy.gitbook.com/book/jaceklaskowski/mastering-apache-spark/details

* Tips for tuning Spark applications:
  - http://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-1/
  - http://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-2/
  - https://dzone.com/articles/apache-spark-performance-tuning-degree-of-parallel


## TODO

* Specify relevant Slurm modules and conda environments in separate application-specific file.
  (currently hardcoded in wrapper script)
