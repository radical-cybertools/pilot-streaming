"""
Ray Cluster Manager

Supports launch via tbd.

"""
import getpass
import logging
import os
import sys
import time
from datetime import datetime
import ray

import numpy as np

logging.getLogger("tornado.application").setLevel(logging.CRITICAL)
logging.getLogger("distributed.utils").setLevel(logging.CRITICAL)

# Resource Managers supported by Dask Pilot-Streaming Plugin
import pilot.job.slurm
import pilot.job.ec2
import pilot.job.ssh
import pilot.job.pilot_os
from urllib.parse import urlparse

from pilot.util.ssh_utils import execute_ssh_command, execute_ssh_command_as_daemon

class Manager():

    def __init__(self, jobid, working_directory):
        self.jobid = jobid
        print("{}{}".format(self.jobid, working_directory))
        self.working_directory = os.path.join(working_directory, jobid)
        self.pilot_compute_description = None
        self.myjob = None  # SAGA Job
        self.local_id = None  # Local Resource Manager ID (e.g. SLURM id)
        self.ray_process = None
        self.ray_cluster = None
        self.job_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.job_output = open(self.job_timestamp + "_ray_pilotstreaming_agent_output.log", "w")
        self.job_error = open(self.job_timestamp + "_ray_pilotstreaming_agent_error.log", "w")

        try:
            os.makedirs(self.working_directory)
        except:
            pass

    # Ray 2.7.0
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
            # create a job service for SLURM LRMS or EC2 Cloud
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

            self.pilot_compute_description = pilot_compute_description

            if url_schema.startswith("slurm"):
                # SLURM plugin
                executable = "python"
                arguments = ["-m", "pilot.plugins.ray.bootstrap_ray", " -p ", str(cores_per_node)]
                if "dask_cores" in pilot_compute_description:
                    arguments = ["-m", "pilot.plugins.ray.bootstrap_ray", " -p ", str(pilot_compute_description["ray_cores"])]
                if extend_job_id != None:
                    arguments = ["-m", "pilot.plugins.ray.bootstrap_ray", "-j", extend_job_id]
                logging.debug("Run %s Args: %s" % (executable, str(arguments)))
            else:
                # EC2 / OS / SSH plugin
                # Boostrap of dask is done after ssh machine is initialized
                executable = "/bin/hostname"  # not required - just starting vms
                arguments = ""  # not required - just starting vms
            jd = {
                "executable": executable,
                "arguments": arguments,
                "working_directory": self.working_directory,
                "output": "dask_job_%s.stdout" % self.jobid,
                "error": "dask_job_%s.stderr" % self.jobid,
                "number_of_nodes": number_of_nodes,
                "cores_per_node": cores_per_node,
                "project": project,
                "reservation": reservation,
                "queue": queue,
                "walltime": walltime,
                "pilot_compute_description": pilot_compute_description
            }
            self.myjob = js.create_job(jd)
            self.myjob.run()
            self.local_id = self.myjob.get_id()
            print("**** Job: " + str(self.local_id) + " State: %s" % (self.myjob.get_state()))
            if not url_schema.startswith(
                    "slurm"):  # Dask is started in SLURM script. This is for ec2, openstack and ssh adaptors
                self.run_dask()  # run dask on cloud platforms

            return self.myjob
        except Exception as ex:
            print("An error occurred: %s" % (str(ex)))
            raise ex

    def run_ray(self):
        ## Run Ray
        self.nodes = self.myjob.get_nodes_list()
        resource_url = self.pilot_compute_description["resource"]
        # self.host = self.myjob.get_nodes_list_public()[0] #first node is master host - requires public ip to connect to
        self.host = self.nodes[0]  # first node is master host - requires public ip to connect to
        self.user = None
        print("Check for user name")
        if urlparse(resource_url).username is not None:
            self.user = urlparse(resource_url).username
            self.pilot_compute_description["os_ssh_username"] = self.user
        elif "os_ssh_username" in self.pilot_compute_description:
            self.user = self.pilot_compute_description["os_ssh_username"]
        else:
            self.user = getpass.getuser()
            self.pilot_compute_description["os_ssh_username"] = self.user

        print("Check for user name*****", self.user)

        print("Check for ssh key")
        self.ssh_key = "~/.ssh/mykey"
        try:
            if "os_ssh_keyfile" in self.pilot_compute_description["os_ssh_keyfile"]:
                self.ssh_key = self.pilot_compute_description["os_ssh_keyfile"]
        except:
            # set to default key for further processing
            self.pilot_compute_description["os_ssh_keyfile"] = self.ssh_key

        worker_options = {"num_cpus": 1, "num_gpus": 0}
        try:
            if "cores_per_node" in self.pilot_compute_description:
                worker_options = {"num_cpus": self.pilot_compute_description["cores_per_node"],
                                  "num_gpus": 0}
        except:
            pass

        # create shell command to start ray head and worker nodes via ssh

        job_id_work_dir = os.path.join(self.working_directory)
        self.executable = "mkdir {}; cd {}; python".format(job_id_work_dir, job_id_work_dir)
        self.arguments = ["-m ", "pilot.plugins.ray.bootstrap_ray"]
        command = "{} {}".format(self.executable, "".join(self.arguments))
        logging.debug("Command {} ".format(command))

        result=execute_ssh_command(host=self.host, user=self.user, command=command, arguments=None,
                            working_directory=self.working_directory,
                            job_output=self.job_output, job_error=self.job_error,
                            keyfile=self.pilot_compute_description["os_ssh_keyfile"])

        hosts = list(np.append(self.nodes[0], self.nodes))
        print("Connecting to hosts", hosts)
        
        print(ray.nodes())
        self.host = ray.nodes()[0]["NodeManagerAddress"]

        if self.host is not None:
            with open(os.path.join(self.working_directory, "ray_scheduler"), "w") as master_file:
                master_file.write(self.host)

    def wait(self):
        while True:
            state = self.myjob.get_state()
            logging.debug("**** Job: " + str(self.local_id) + " State: %s" % (state))
            if state.lower() == "running":
                logging.debug("Looking for Dask startup state at: %s" % self.working_directory)
                if self.is_scheduler_started():
                    for i in range(5):
                        try:
                            print("init distributed client")
                            c = self.get_context()
                            # c.scheduler_info()
                            print(str(c.scheduler_info()))
                            c.close()

                            return
                        except IOError as e:
                            print("Dask Client Connect Attempt {} failed".format(i))
                            time.sleep(5)
            elif state == "Failed":
                break
            time.sleep(6)

    def cancel(self):
        c = self.get_context()
        c.run_on_scheduler(lambda dask_scheduler=None: dask_scheduler.close() & sys.exit(0))
        self.dask_cluster.close()
        #self.myjob.cancel()

    def submit_compute_unit(function_name):
        pass

    def get_context(self, configuration=None) -> object:
        """Returns Dask Client for Scheduler"""
        details = self.get_config_data()
        if details is not None:
            print("Connect to Dask: %s" % details["master_url"])
            client = distributed.Client(details["master_url"])
            return client
        return None

    def get_jobid(self):
        return self.jobid

    def get_config_data(self):
        if not self.is_scheduler_started():
            logging.debug("Scheduler not started")
            return None
        master_file = os.path.join(self.working_directory, "dask_scheduler")
        # print master_file
        master = "localhost"
        counter = 0
        while os.path.exists(master_file) == False and counter < 600:
            time.sleep(2)
            counter = counter + 1

        with open(master_file, 'r') as f:
            master = f.read()

        if master.startswith("tcp://"):
            details = {
                "master_url": master
            }
        else:
            master_host = master.split(":")[0]
            details = {
                "master_url": "tcp://%s:8786" % master_host,
                "web_ui_url": "http://%s:8787" % master_host,
            }
        return details

    def print_config_data(self):
        details = self.get_config_data()
        print("Dask Scheduler: %s" % details["master_url"])

    def is_scheduler_started(self):
        logging.debug("Results of scheduler startup file check: %s" % str(
            os.path.exists(os.path.join(self.working_directory, "dask_scheduler"))))
        return os.path.exists(os.path.join(self.working_directory, "dask_scheduler"))
