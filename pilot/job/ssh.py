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

from pilot.api import Service
from pilot.job.state import State

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


class SSHService(Service):
    """ Plugin for submitting Pilot on remote hosts thru SSH Protocol
        Manages endpoint in the form of: ssh://<Host>
    """

    def __init__(self, resource_url):
        super().__init__(resource_url)

    def create_pilot(self, job_description):
        return Job(job_description, self.resource_url)

class Job(object):
    """ Plugin for SSH (to execute defined command on remote machine) """

    def __init__(self, job_description, resource_url):
        self.resource_url = resource_url
        self.job_description = job_description

        self.host = urlparse(resource_url).hostname
        self.user = os.environ.get("USER")
        if urlparse(resource_url).username is not None:
            self.user = urlparse(resource_url).username

        logger.debug("Start pilot with resourceUrl: {}, jobDescription: {}", self.resource_url, self.job_description)

        self.job_id = "pilot-streaming-ssh" + str(uuid.uuid1())
        self.job_output = open("%s_output.log" % self.job_id, "w")
        self.job_error = open("%s_error.log" % self.job_id, "w")
        self.working_directory = job_description["working_directory"]

    def run(self):
        # Create working directory.
        os.makedirs(self.working_directory, exist_ok=True)

        TRIAL_MAX = 3
        trials = 0
        job_state = None
        while trials < TRIAL_MAX:
            try:
                job_state = self.check_vm_running()
                if not job_state:
                    trials = trials + 1
                    time.sleep(30)
                    continue
                else:
                    break
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                logger.warning("Submission failed: " + str(exc_value) + str(exc_traceback))
                # self.__print_traceback()
                trials = trials + 1
                time.sleep(3)
                if trials == TRIAL_MAX:
                    raise Exception("Submission of agent failed.")

        logger.debug("Job State : %s" % job_state)
        self.run_command()

    def check_vm_running(self):
        args = []
        args.extend(["ssh", "-l", self.user, self.host, "/bin/date"])
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

    def get_id(self):
        return self.job_id

    def get_state(self):
        try:
            running = self.check_vm_running()
            if running:
                return State.RUNNING
            else:
                return State.UNKNOWN
        except:
            logger.warning("Instance not reachable/active yet...")

    def cancel(self):
        if self.subprocess_handle != None: self.subprocess_handle.terminate()
        self.job_output.close()
        self.job_error.close()

    def get_nodes_list(self):
        return [self.host]  # only single host via SSH

    def get_node_list(self):
        self.get_nodes_list()

    def get_nodes_list_public(self):
        self.get_nodes_list()

    def run_command(self):

        if self.user is not None:
            command = "ssh -o 'StrictHostKeyChecking=no' -l %s %s -t \"bash -ic '%s %s'\"" % \
                      (self.user, self.host,
                       str(self.pilot_compute_description["executable"]),
                       " ".join(self.pilot_compute_description["arguments"]))
        else:
            command = "ssh -o 'StrictHostKeyChecking=no'  %s -t \"bash -ic '%s %s'\"" % \
                      (self.host,
                       str(self.pilot_compute_description["executable"]),
                       " ".join(self.pilot_compute_description["arguments"]))

        print("Execute SSH Command: {0}".format(command))
        # status = subprocess.call(command, shell=True)
        for i in range(3):
            self.ssh_process = subprocess.Popen(command, shell=True,
                                                cwd=self.working_directory,
                                                stdout=self.job_output,
                                                stderr=self.job_error,
                                                close_fds=True)
            time.sleep(10)
            if self.ssh_process.poll is not None:
                break

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
    job_service = SSHService("ssh://localhost")
    job = job_service.create_job(job_description)
    job.run()
    print(job.get_state())
