# Pilot-Streaming

Last Updated: 03/24/2019

# Overview:

 Pilot-Streaming is a tool to manage Streaming environment 
 consisting of Kafka, Spark Streaming, Flink and Dask on HPC systems. 
 It is based on the [SAGA-Hadoop](http://github.com/drelu/saga-hadoop) tool and extends 
 it for streaming.

Currently supported SAGA adaptors:

- Fork
- Torque

Requirements:

	* PBS/Torque cluster
	* Working directory should be on a shared filesystem
	* Setup password-less documentation
	* JAVA needs to be installed and in PATH


Anaconda is the preferred distribution


# Usage

Requirement (in case a manual installation is required):

The best way to utilize Pilot-Streaming is Anaconda, which provides an easy way to install
important dependencies (such as PySpark and Dask). Make sure the PySpark version is compabitible 
with the Pilot-Streaming version (currently 2.2.1).

    conda install pyspark -c conda-forge 
    conda install -c conda-forge pykafka
    conda install paramiko distributed 
    
    pip install --upgrade saga-python

Try to run a local Hadoop (e.g. for development and testing)
	
	
    pip install --upgrade .
    psm --resource fork://localhost
    
    
Try to run a Hadoop cluster inside a PBS/Torque job:

    psm --resource pbs+ssh://india.futuregrid.org --number_cores 8

Some Blog Posts about SAGA-Hadoop:

    * <http://randomlydistributed.blogspot.com/2011/01/running-hadoop-10-on-distributed.html>


# Packages:

see `hadoop1` for setting up a Hadoop 1.x.x cluster

see `hadoop2` for setting up a Hadoop 2.7.x cluster
 
see `spark` for setting up a Spark 2.2.x cluster

see `kafka` for setting up a Kafka 1.0.x cluster

see `flink` for setting up a Flink 1.1.4 cluster

see `dask` for setting up a Dask Distributed 1.20.2 cluster


# Examples:


***Stampede:***

    psm --resource=slurm://localhost --queue=normal --walltime=239 --number_cores=256 --project=xxx


***Gordon:***

    psm --resource=pbs://localhost --walltime=59 --number_cores=16 --project=TG-CCR140028 --framework=spark
    

***Wrangler***

    export JAVA_HOME=/usr/java/jdk1.8.0_45/
    psm --resource=slurm://localhost --queue=normal --walltime=59 --number_cores=24 --project=xxx


