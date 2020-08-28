#!/usr/bin/env python
# coding: utf-8

# # Getting Started with Pilot-Streaming and Edge on LRZ (Jetstream/TACC WIP)
# 
# In the first step we need to import all required packages and modules into the Python Path

# Pilot-Streaming can be used to manage the Dask and Kafka environments both in the cloud and on the edge. 
# 
# 
# 
# `resource`: URL of the Local Resource Manager. Examples:
# 
# * `slurm://localhost`: Submit to local SLURM resource manager, e.g. on master node of Wrangler or Stampede
# * `slurm+ssh://login1.wrangler.tacc.utexas.edu`: Submit to Wrangler master node SLURM via SSH (e.g. on node running a job)
# * `os://` Openstack
# * `ec2://` EC2
# 
# 
# `type:` The `type` attributes specifies the cluster environment. It can be: `Spark`, `Dask` or `Kafka`.
# 
# 
# Depending on the resource there might be other configurations necessary, e.g. to ensure that the correct subnet is used the Spark driver can be configured using various environment variables:   os.environ["SPARK_LOCAL_IP"]='129.114.58.2'
# 
# 
import os, sys
import distributed
import json
import pilot.streaming
import socket
# configure logging
import logging


logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger("stevedore.extension").setLevel(logging.CRITICAL)
logging.getLogger("keystoneauth").setLevel(logging.CRITICAL)
logging.getLogger("urllib3.connectionpool").setLevel(logging.CRITICAL)
logging.getLogger("asyncio").setLevel(logging.CRITICAL)
sys.modules['pilot.streaming']

RESOURCE_URL_HPC = "slurm+ssh://login4.stampede2.tacc.utexas.edu"
WORKING_DIRECTORY = os.path.join(os.environ["HOME"], "work")
RESOURCE_URL_EDGE = "ssh://aluckow@js-17-136.jetstream-cloud.org"

# RESOURCE_URL_EDGE="os://cc.lrz.de"
# RESOURCE_URL_EDGE="ssh://localhost"


pilot_compute_description = {
    "resource": RESOURCE_URL_EDGE,
    "working_directory": WORKING_DIRECTORY,
    "number_of_nodes": 1,
    "cores_per_node": 48,
    "project": "TG-MCB090174",
    "queue": "normal",
    "config_name": "jetstream",
    "walltime": 59,
    "type": "kafka"
}

# kafka_pilot = pilot.streaming.PilotComputeService.create_pilot(pilot_compute_description)
# kafka_pilot.wait()
# print(kafka_pilot.get_details())

#######################################################################################################################
# Dask Edge
pilot_compute_description = {
    "resource": RESOURCE_URL_EDGE,
    "working_directory": WORKING_DIRECTORY,
    "number_of_nodes": 1,
    "cores_per_node": 2,
    "dask_cores": 2,
    "type": "dask"
}

dask_pilot = pilot.streaming.PilotComputeService.create_pilot(pilot_compute_description)
dask_pilot.wait()
print(dask_pilot.get_details())

#######################################################################################################################
# # Dask
# pilot_compute_description = {
#     "resource":RESOURCE_URL_HPC,
#     "working_directory": WORKING_DIRECTORY,
#     "number_of_nodes": 1,
#     "cores_per_node": 48,
#     "dask_cores" : 24,
#     "project": "TG-MCB090174",
#     "queue": "normal",
#     "walltime": 359,
#     "type":"dask"
# }
#
#
#
# dask_pilot = pilot.streaming.PilotComputeService.create_pilot(pilot_compute_description)
# dask_pilot.wait()
# dask_pilot.get_details()

dask_client = distributed.Client(dask_pilot.get_details()['master_url'])
# dask_client  = distributed.Client()
print(str(dask_client.scheduler_info()))
print(str(dask_client.gather(dask_client.map(lambda a: a * a, range(10)))))
print(str(dask_client.gather(dask_client.map(lambda a: socket.gethostname(), range(10)))))

dask_pilot.cancel()
# kafka_pilot.cancel()