# System Libraries
import sys, os
import pandas as pd

## logging
import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger().setLevel(logging.ERROR)
logging.getLogger("py4j").setLevel(logging.ERROR)
 

# Pilot-Streaming
import pilot.streaming
sys.modules['pilot.streaming']

RESOURCE_URL_DASK = "ssh://aluckow@js-129-114-17-61.jetstream-cloud.org"
RESOURCE_URL_KAFKA = "ssh://aluckow@js-129-114-17-61.jetstream-cloud.org"
WORKING_DIRECTORY = os.path.join(os.environ["HOME"], "work")

pilot_compute_description = {
    "resource": RESOURCE_URL_KAFKA,
    "working_directory": WORKING_DIRECTORY,
    "number_of_nodes": 1,
    "cores_per_node": 1,
    "config_name": "jetstream",
    "walltime": 59,
    "type": "kafka"
}
kafka_pilot = pilot.streaming.PilotComputeService.create_pilot(pilot_compute_description)
kafka_pilot.wait()
