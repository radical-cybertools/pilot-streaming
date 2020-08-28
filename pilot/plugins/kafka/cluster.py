"""
Kafka Cluster Manager
"""
import logging
# import saga
import os
import time
from urllib.parse import urlparse

import pilot
from pilot.job.slurm import Service
from ...job.ssh import State


class Manager:

    def __init__(self, jobid, working_directory):
        self.jobid = jobid
        self.working_directory = os.path.join(working_directory, jobid)
        self.host = None
        self.ssh_job = None  # Handle to SSH Job
        self.local_id = None  # Local Resource Manager ID (e.g. SLURM id)
        try:
            os.makedirs(os.path.join(self.working_directory, 'config'))
        except:
            pass

    # Kafka 2.6.x
    def submit_job(self,
                   resource_url="fork://localhost",
                   number_of_nodes=1,
                   number_cores=1,
                   cores_per_node=1,
                   spmd_variation=None,
                   queue=None,
                   walltime=None,
                   project=None,
                   reservation=None,
                   config_name="default",
                   extend_job_id=None,
                   pilot_compute_description=None
                   ):
        try:
            # create a job service for SLURM LRMS
            # js = Service(resource_url)
            url_schema = urlparse(resource_url).scheme
            js = None
            if url_schema.startswith("slurm"):
                js = pilot.job.slurm.Service(resource_url)
            elif url_schema.startswith("ec2"):
                js = pilot.job.ec2.Service(resource_url)
            elif url_schema.startswith("os"):
                js = pilot.job.os.Service(resource_url)
            elif url_schema.startswith("ssh"):
                js = pilot.job.ssh.Service(resource_url)
            else:
                print("Unsupported URL Schema: %s " % resource_url)
                return

            self.host = urlparse(resource_url).hostname
            # environment, executable & arguments
            executable = "mkdir {}; cd {}; python".format(self.jobid, self.jobid)
            arguments = ["-m ", "pilot.plugins.kafka.bootstrap_kafka", " -n ", config_name]
            if extend_job_id != None:
                arguments = ["-m", "pilot.plugins.kafka.bootstrap_kafka", "-j", extend_job_id]
            logging.debug("Run %s Args: %s" % (executable, str(arguments)))

            jd = {
                "executable": executable,
                "arguments": arguments,
                "working_directory": self.working_directory,
                "output": "kafka_job_%s.stdout" % self.jobid,
                "error": "kafka_job_%s.stderr" % self.jobid,
                "number_of_nodes": number_of_nodes,
                "cores_per_node": cores_per_node,
                "project": project,
                "reservation": reservation,
                "queue": queue,
                "walltime": walltime,
                "pilot_compute_description": pilot_compute_description
            }
            self.ssh_job = js.create_job(jd)
            self.ssh_job.run()
            self.local_id = self.ssh_job.get_id()
            print("**** Job: " + str(self.local_id) + " State : %s" % (self.ssh_job.get_state()))
            if self.ssh_job.get_state() == State.RUNNING:
                with open(os.path.join(self.working_directory, "kafka_started"), "w") as master_file:
                    master_file.write(self.host + ":8786")
            return self.ssh_job
        except Exception as ex:
            print("An error occurred: %s" % (str(ex)))

    def wait(self):
        while True:
            state = self.ssh_job.get_state()
            logging.debug(
                "**** Job: " + str(self.local_id) + " State: %s" % state + " isRunning: %s" % (state == State.RUNNING))
            if state == State.RUNNING:
                logging.debug("looking for Kafka startup state at: %s" % self.working_directory)
                if os.path.exists(os.path.join(self.working_directory, "kafka_started")):
                    break
            elif state == State.FAILED:
                break
            time.sleep(3)

    def submit_compute_unit(function_name):
        pass

    def get_jobid(self):
        return self.jobid

    def get_context(self):
        pass

    def get_config_data(self):
        self.wait()
        kafka_config = {}
        conf = os.path.join(self.working_directory, "config")
        print("look for configs in: " + conf)
        broker_config_dirs = [i if os.path.isdir(os.path.join(conf, i)) and i.find("broker-") >= 0 else None for i in
                                  os.listdir(conf)]
        broker_config_dirs = [a for a in broker_config_dirs if a != None]
        print(str(broker_config_dirs))

        if len(broker_config_dirs)>0:  # read config from generated config files (from bootstrap_kafka.py)
            for broker in broker_config_dirs:
                with open(os.path.join(conf, broker, "server.properties"), "r") as config:
                    print("Kafka Config: %s (%s)" % (conf, time.ctime(os.path.getmtime(conf))))
                    lines = config.readlines()
                    for line in lines:
                        if line.startswith("broker.id") or line.startswith("listeners") or line.startswith(
                                "zookeeper.connect"):
                            # print line.strip().replace("=", ": ")
                            line_comp = line.split("=")
                            kafka_config[line_comp[0].strip()] = line_comp[1].strip()
        else: #generate default config from hostname
            kafka_config["zookeeper.connect"]="{}:2181".format(self.host)

        print(str(kafka_config))
        details = {"master_url": kafka_config["zookeeper.connect"],
                   "details": kafka_config}
        return details

    def print_config_data(self):
        details = self.get_config_data()
        print("Zookeeper: %s" % details["master_url"])
