#!/usr/bin/env python

import datetime
import logging
import os
import subprocess
import sys
import time
import traceback
import uuid
from urllib.parse import urlparse

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


class State:
    UNKNOWN = "unknown"
    PENDING = "pending"
    RUNNING = "running"
    FAILED = "failed"
    DONE = "done"


class Service(object):
    """ Plugin for Amazon EC2 and EUCA

        Manages endpoint in the form of:

            ec2+ssh://<EC2 Endpoint>
            euca+ssh://<EUCA Endpoint>
    """

    def __init__(self, resource_url, pilot_compute_description=None):
        """Constructor"""
        self.resource_url = resource_url
        self.pilot_compute_description = pilot_compute_description

    def create_job(self, job_description):
        if "pilot_compute_description" in job_description:
            self.pilot_compute_description = job_description["pilot_compute_description"]
        j = Job(job_description, self.resource_url, self.pilot_compute_description)
        return j

    def __del__(self):
        pass


class Job(object):
    """ Plugin for SSH (currently executes Dask)

        Executes Dask agent on node

    """

    def __init__(self, job_description, resource_url, pilot_compute_description):

        self.job_description = job_description
        self.resource_url = resource_url
        if pilot_compute_description == None:
            self.pilot_compute_description = job_description
        else:
            self.pilot_compute_description = pilot_compute_description
        self.host = urlparse(self.resource_url).netloc
        logger.debug("URL: " + str(self.resource_url) + " Host: " + self.host)
        self.id = "pilot-streaming-ssh" + str(uuid.uuid1())
        self.job_id = self.id
        self.job_timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        self.job_output = open("pilotstreaming_agent_ssh_output_" + self.job_timestamp + ".log", "w")
        self.job_error = open("pilotstreaming_agent_ssh_error_" + self.job_timestamp + ".log", "w")

    def run(self):
        """ Start VMs"""
        # Submit job
        self.working_directory = os.getcwd()
        if "working_directory" in self.job_description:
            self.working_directory = self.job_description["working_directory"]
            print("Working Directory: %s" % self.working_directory)
            try:
                os.makedirs(self.working_directory, exist_ok=True)
            except:
                pass

        TRIAL_MAX = 3
        trials = 0
        while trials < TRIAL_MAX:
            try:
                running = self.check_vm_running()
                if not running:
                    trials = trials + 1
                    time.sleep(30)
                    continue
                else:
                    break
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                logger.warning("Submission failed: " + str(exc_value))
                # self.__print_traceback()
                trials = trials + 1
                time.sleep(3)
                if trials == TRIAL_MAX:
                    raise Exception("Submission of agent failed.")

        logger.debug("Job State : %s" % (self.get_state()))
        # self.run_dask()

    def check_vm_running(self):
        args = []
        args.extend(["ssh", self.host, "/bin/date"])
        logger.debug("Execute: " + str(args))
        subprocess_handle = subprocess.Popen(args=args,
                                             stdout=self.job_output,
                                             stderr=self.job_error,
                                             cwd=self.working_directory,
                                             shell=False)
        running = False
        if subprocess_handle.poll() is not None and subprocess_handle.poll() != 0:
            logger.warning("Submission failed.")
        else:
            logger.debug("Test Job succeeded")
            running = True
        subprocess_handle.kill()
        return running

    def wait_for_running(self, node):
        pass

    # def run_dask(self):
    #     """TODO Move Dask specific stuff into Dask plugin"""
    #     nodes = self.get_node_list()
    #     ## Update Mini Apps
    #     for i in nodes:
    #         self.wait_for_running(i)
    #         command = "ssh -o 'StrictHostKeyChecking=no' -i {} {}@{} pip install --upgrade git+ssh://git@github.com/radical-cybertools/streaming-miniapps.git".format(
    #             self.pilot_compute_description["ec2_ssh_keyfile"],
    #             self.pilot_compute_description["ec2_ssh_username"],
    #             i)
    #         print("Host: {} Command: {}".format(i, command))
    #         install_process = subprocess.Popen(command, shell=True, cwd=self.working_directory)
    #         install_process.wait()
    #
    #     ## Run Dask
    #     command = "dask-ssh --ssh-private-key %s --ssh-username %s --remote-dask-worker distributed.cli.dask_worker %s" % (
    #         self.pilot_compute_description["ec2_ssh_keyfile"],
    #         self.pilot_compute_description["ec2_ssh_username"], " ".join(nodes))
    #     if "cores_per_node" in self.pilot_compute_description:
    #         command = "dask-ssh --ssh-private-key %s --ssh-username %s --nthreads %s --remote-dask-worker distributed.cli.dask_worker %s" % (
    #             self.pilot_compute_description["ec2_ssh_keyfile"], self.pilot_compute_description["ec2_ssh_username"],
    #             str(self.pilot_compute_description["cores_per_node"]), " ".join(nodes))
    #     print("Start Dask Cluster: " + command)
    #     # status = subprocess.call(command, shell=True)
    #     for i in range(3):
    #         self.dask_process = subprocess.Popen(command, shell=True, cwd=self.working_directory, close_fds=True)
    #         time.sleep(10)
    #         if self.dask_process.poll != None:
    #             with open(os.path.join(self.working_directory, "dask_scheduler"), "w") as master_file:
    #                 master_file.write(nodes[0] + ":8786")
    #             break

    def get_id(self):
        return self.job_id

    def get_state(self):
        try:
            running = self.check_vm_running()
            if running:
                return State.RUNNING
            else:
                return State.UNKNOWN
            # result = State.UNKNOWN
            # try:
            #     if self.dask_process != None:
            #         rc = self.dask_process.poll()
            #         if rc == None:
            #             result = State.RUNNING
            #         elif rc != 0:
            #             result = State.FAILED
            #         elif rc == 0:
            #             result = State.DONE
        except:
            logger.warning("Instance not reachable/active yet...")

    def cancel(self):
        if self.subprocess_handle != None: self.subprocess_handle.terminate()
        self.job_output.close()
        self.job_error.close()

    def get_node_list(self):
        return ["localhost"]  # not yet available on manager side for slurm

    # def run_dask(self):
    #     """TODO Move Dask specific stuff into Dask plugin"""
    #     ## Update Mini Apps
    #     # for i in nodes:
    #     #    self.wait_for_ssh(i)
    #     #    command = "ssh -o 'StrictHostKeyChecking=no' -i {} {}@{} pip install --upgrade git+ssh://git@github.com/radical-cybertools/streaming-miniapps.git".format(self.pilot_compute_description["ec2_ssh_keyfile"],
    #     #                                        self.pilot_compute_description["ec2_ssh_username"],
    #     #                                        i)
    #     #    print("Host: {} Command: {}".format(i, command))
    #     #    install_process = subprocess.Popen(command, shell=True, cwd=self.working_directory)
    #     #    install_process.wait()
    #
    #     ## Run Dask
    #     # command = "dask-ssh --remote-dask-worker distributed.cli.dask_worker %s"%(self.host)
    #     command = "ssh %s dask-ssh %s" % (self.host, "localhost")
    #     if "cores_per_node" in self.pilot_compute_description:
    #         # command = "dask-ssh --nthreads %s --remote-dask-worker distributed.cli.dask_worker %s"%\
    #         command = "ssh %s dask-ssh --nthreads %s %s" % \
    #                   (self.host, str(self.pilot_compute_description["cores_per_node"]), "localhost")
    #     print("Start Dask Cluster: {0}".format(command))
    #     # status = subprocess.call(command, shell=True)
    #     for i in range(3):
    #         self.dask_process = subprocess.Popen(command, shell=True,
    #                                              cwd=self.working_directory,
    #                                              stdout=self.job_output,
    #                                              stderr=self.job_error,
    #                                              close_fds=True)
    #         time.sleep(10)
    #         if self.dask_process.poll != None:
    #             with open(os.path.join(self.working_directory, "dask_scheduler"), "w") as master_file:
    #                 master_file.write(self.host + ":8786")
    #             break

    ###########################################################################
    # private methods
    def __print_traceback(self):
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print("*** print_tb:")
        traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
        print("*** print_exception:")
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                                  limit=2, file=sys.stdout)


if __name__ == "__main__":
    RESOURCE_URL_EDGE = "ssh://localhost"
    WORKING_DIRECTORY_EDGE = "/home/aluckow"
    job_description = {
        "resource": RESOURCE_URL_EDGE,
        "working_directory": WORKING_DIRECTORY_EDGE,
        "number_of_nodes": 1,
        "cores_per_node": 1,
        "dask_cores": 2,
        "project": "TG-MCB090174",
        "queue": "normal",
        "walltime": 359,
        "type": "dask"
    }
    job_service = Service("ssh://localhost")
    job = job_service.create_job(job_description)
    job.run()
    print(job.get_state())
