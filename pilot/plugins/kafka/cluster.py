"""
Kafka Cluster Manager
"""
import logging
# import saga
import os
import subprocess
import time
from datetime import datetime
from urllib.parse import urlparse

import pilot
from pilot.job.slurm import Service
from ...job.ssh import State


class Manager:

    def __init__(self, jobid, working_directory):
        self.jobid = jobid
        self.working_directory = os.path.join(working_directory, jobid)
        self.executable = "mkdir {}; cd {}; python".format(self.jobid, self.jobid)
        self.job_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.job_output = open(self.job_timestamp + "_kafka_pilotstreaming_agent_output.log", "w")
        self.job_error = open(self.job_timestamp + "_kafka_pilotstreaming_agent_error.log", "w")
        self.host = None
        self.user = None
        self.pilot_job = None  # Handle to SSH Job
        self.local_id = None  # Local Resource Manager ID (e.g. SLURM id)
        self.config_name ="default"
        self.extend_job_id = None
        self.pilot_compute_description = None
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
            self.pilot_compute_description = pilot_compute_description
            self.config_name=config_name
            url_schema = urlparse(resource_url).scheme
            print("Kafka Plugin for Job Type: {}".format(url_schema))

            # select appropriate adaptor for creation of pilot job
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
            self.config_name = config_name
            self.host = urlparse(resource_url).hostname

            if url_schema.startswith("slurm"):
                # environment, executable & arguments
                executable = "mkdir {}; cd {}; python".format(self.jobid, self.jobid)
                arguments = ["-m ", "pilot.plugins.kafka.bootstrap_kafka", " -n ", self.config_name]
                self.extend_job_id = extend_job_id
                if self.extend_job_id is not None:
                    arguments = ["-m", "pilot.plugins.kafka.bootstrap_kafka", "-j", extend_job_id]
            else: # cloud
                # EC2 / OS / SSH plugin
                # Boostrap of dask is done after ssh machine is initialized
                executable = "/bin/hostname"  # not required - just starting vms
                arguments = ""  # not required - just starting vms

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
            self.pilot_job = js.create_job(jd)
            self.pilot_job.run()
            self.local_id = self.pilot_job.get_id()
            print("**** Job: " + str(self.local_id) + " State : %s" % (self.pilot_job.get_state()))
            if self.pilot_job.get_state() == State.RUNNING:
                if not url_schema.startswith("slurm"):
                    self.run_kafka()
                with open(os.path.join(self.working_directory, "kafka_started"), "w") as master_file:
                    master_file.write(self.host + ":9092")
            return self.pilot_job
        except Exception as ex:
            print("An error occurred: %s" % (str(ex)))

    def run_kafka(self):
        """
        For cloud adaptors this method is running the bootstrap script on the first node allocated
        by the cloud job adaptor.
        :return:
        """
        resource_url = self.pilot_compute_description["resource"]
        # get public and private IPs of nodes
        self.nodes = self.pilot_job.get_nodes_list()
        self.host = self.pilot_job.get_nodes_list_public()[0]  # first node is master host - requires public ip to connect to

        if urlparse(resource_url).username is not None:
            self.user = urlparse(resource_url).username
        elif "os_ssh_username" in self.pilot_compute_description:
            self.user = self.pilot_compute_description["os_ssh_username"]

        # install pilot-streaming

        self.install_pilot_streaming()

        self.executable = "mkdir {}; cd {}; python".format(self.jobid, self.jobid)
        self.arguments = ["-m ", "pilot.plugins.kafka.bootstrap_kafka", " -n ", self.config_name]
        if self.extend_job_id is not None:
            self.arguments = ["-m", "pilot.plugins.kafka.bootstrap_kafka", "-j", self.extend_job_id]
        command = "{} {}".format(self.executable, "".join(self.arguments))
        logging.debug("Command {} ".format(command))

        if self.user is not None:
            # command = "dask-ssh --nthreads %s --remote-dask-worker distributed.cli.dask_worker %s"%\
            command = "ssh -o 'StrictHostKeyChecking=no' -l %s %s -t \"bash -ic '%s'\"" % \
                      (self.user, self.host, command)
        else:
            command = "ssh -o 'StrictHostKeyChecking=no' %s -t \"bash -ic '%s'\"" % \
                      (self.host, command)

        print("Start Kafka Cluster: {0}".format(command))
        # status = subprocess.call(command, shell=True)
        for i in range(3):
            self.kafka_process = subprocess.Popen(command, shell=True,
                                                  cwd=self.working_directory,
                                                  stdout=self.job_output,
                                                  stderr=self.job_error,
                                                  close_fds=True)
            time.sleep(10)
            if self.kafka_process.poll is not None:
                break

    def install_pilot_streaming(self, hostname):
        """
        Installs and bootstraps latest pilot-streaming and mini apps from github
        :param hostname:
        :return:
        """
        #
        command = "ssh -o 'StrictHostKeyChecking=no' -i {} {}@{} pip install --upgrade git+ssh://git@github.com/radical-cybertools/pilot-streaming.git".format(
            self.pilot_compute_description["os_ssh_keyfile"],
            self.pilot_compute_description["os_ssh_username"],
            hostname)
        print("Host: {} Command: {}".format(hostname, command))
        install_process = subprocess.Popen(command, shell=True, cwd=self.working_directory)
        install_process.wait()

        # MINI Apps
        command = "ssh -o 'StrictHostKeyChecking=no' -i {} {}@{} pip install --upgrade git+ssh://git@github.com/radical-cybertools/streaming-miniapps.git".format(
                     self.pilot_compute_description["os_ssh_keyfile"],
                     self.pilot_compute_description["os_ssh_username"],
                     hostname)
        print("Host: {} Command: {}".format(hostname, command))
        install_process = subprocess.Popen(command, shell=True, cwd=self.working_directory)
        install_process.wait()

    def wait(self):
        while True:
            state = self.pilot_job.get_state()
            logging.debug(
                "**** Job: " + str(self.local_id) + " State: %s" % state + " isRunning: %s" % (state == State.RUNNING))
            if state == State.RUNNING:
                break
                #logging.debug("looking for Kafka startup state at: %s" % self.working_directory)
                #if os.path.exists(os.path.join(self.working_directory, "kafka_started")):
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
        else: #generate default config from hostname with default ports ###fallback
            kafka_config["master_url"]="{}:2181".format(self.host)
            kafka_config["zookeeper.connect"]="{}:2181".format(self.host)
            kafka_config["listeners"]="{}:9092".format(self.host)

        print(str(kafka_config))
        details = {"master_url": kafka_config["zookeeper.connect"],
                   "details": kafka_config}
        return details

    def print_config_data(self):
        details = self.get_config_data()
        print("Zookeeper: %s" % details["master_url"])
