#!/usr/bin/env python

# OpenStack Pluging for allocating VMs
import datetime
import math
import os
import subprocess
import sys
import time
import traceback
import uuid
from keystoneauth1 import loading
from keystoneauth1 import session
import openstack
from novaclient.client import Client


class State:
    UNKNOWN = "Unknown"
    PENDING = "Pending"
    RUNNING = "Running"
    FAILED = "Failed"
    DONE = "done"


class Service(object):
    """ Plugin for Amazon EC2 and EUCA

        Manages endpoint in the form of:

            ec2+ssh://<EC2 Endpoint>
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
    """ Plugin for OpenStack

        Starts OpenStack VM and executes Dask agent on this VM

    """

    def __init__(self, job_description, resource_url, pilot_compute_description):

        self.job_description = job_description
        print("URL: " + str(resource_url) + " Type: " + str(type(resource_url)))
        self.resource_url = resource_url
        self.pilot_compute_description = pilot_compute_description

        self.id = "pilotstreaming-" + str(uuid.uuid1())
        self.job_id = self.id
        self.job_timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        # self.job_output = open("pilotstreaming_agent_output_"+self.job_timestamp+".log", "w")
        # self.job_error = open("pilotstreaming_agent_output__agent_error_"+self.job_timestamp+".log", "w")

        # OpenStack related variables
        loader = loading.get_plugin_loader('password')
        auth = loader.load_from_options(auth_url=self.pilot_compute_description["os_auth_url"],
                                        username=self.pilot_compute_description["os_username"],
                                        password=self.pilot_compute_description["os_password"],
                                        default_domain_name=self.pilot_compute_description["os_user_domain"],
                                        project_id=self.pilot_compute_description["os_project_id"])
        self.sess = session.Session(auth=auth)
        self.conn = openstack.connection.Connection(session=self.sess)
        self.server = None  # Handle to pilot-edge VM
        self.ip = None  # public IP of server
        self.job_id = self.id

    def run_os_instances(self):
        print(str(self.pilot_compute_description))
        self.server = self.conn.create_server(name=self.pilot_compute_description["os_name"],
                                              image=self.pilot_compute_description["os_image_id"],
                                              flavor=self.pilot_compute_description["os_instance_type"],
                                              key_name=self.pilot_compute_description["os_ssh_keyname"],
                                              security_groups=[self.pilot_compute_description["os_security_group"]],
                                              auto_ip=True,
                                              ips=None,
                                              ip_pool=None,
                                              root_volume=None,
                                              terminate_volume=True,
                                              wait=False,
                                              timeout=180,
                                              reuse_ips=True,
                                              network=self.pilot_compute_description["os_network"],
                                              boot_from_volume=True,
                                              volume_size='20',
                                              boot_volume=None,
                                              volumes=None,
                                              nat_destination=None,
                                              group=None)

        print("Finished creating Instance")
        self.wait_for_running()
        self.ip = self.conn.create_floating_ip(network=self.pilot_compute_description["os_network_floating_ip"],
                                               server=self.server)
        # if "type" in self.pilot_compute_description and self.pilot_compute_description["type"] == "dask":
        #     """TODO Move Dask specific stuff into Dask plugin"""
        #     print("Run Dask")
        #     time.sleep(30)
        #     self.run_dask()

    def run(self):
        """ Start VMs """
        # Submit job
        print("Start OpenStack VMs")
        self.working_directory = os.getcwd()
        if "working_directory" in self.job_description:
            self.working_directory = self.job_description["working_directory"]
            print("Working Directory: %s" % self.working_directory)
            try:
                os.makedirs(self.working_directory, exist_ok=True)
            except:
                pass

        self.run_os_instances()
        #self.wait_for_running()
        #self.wait_for_ssh()
        return self

    def wait_for_ssh(self, node):
        # check ssh login
        for i in range(10):
            try:
                command = "ssh -o 'StrictHostKeyChecking=no' -i {} {}@{} /bin/echo 1".format(
                    self.pilot_compute_description["os_ssh_keyfile"],
                    self.pilot_compute_description["os_ssh_username"],
                    node)
                print("Host: {} Command: {}".format(node, command))
                output = subprocess.check_output(command, shell=True, cwd=self.working_directory)
                print(output.decode("utf-8"))
                if output.decode("utf-8").startswith("1"):
                    print("Test successful")
                    return
            except:
                pass
            time.sleep(math.pow(2, i))

    # def run_dask(self):
    #     """TODO Move Dask specific stuff into Dask plugin"""
    #     nodes = self.get_node_list()
    #     ## Update Mini Apps
    #     for i in nodes:
    #         self.wait_for_ssh(i)
    #         command = "ssh -o 'StrictHostKeyChecking=no' -i {} {}@{} pip install --upgrade git+ssh://git@github.com/radical-cybertools/streaming-miniapps.git".format(
    #             self.pilot_compute_description["os_ssh_keyfile"],
    #             self.pilot_compute_description["os_ssh_username"],
    #             i)
    #         print("Host: {} Command: {}".format(i, command))
    #         install_process = subprocess.Popen(command, shell=True, cwd=self.working_directory)
    #         install_process.wait()
    #
    #     ## Run Dask
    #     command = "dask-ssh --ssh-private-key %s --ssh-username %s --remote-dask-worker distributed.cli.dask_worker %s" % (
    #         self.pilot_compute_description["os_ssh_keyfile"],
    #         self.pilot_compute_description["os_ssh_username"], " ".join(nodes))
    #     if "cores_per_node" in self.pilot_compute_description:
    #         command = "dask-ssh --ssh-private-key %s --ssh-username %s --nthreads %s --remote-dask-worker distributed.cli.dask_worker %s" % (
    #             self.pilot_compute_description["os_ssh_keyfile"], self.pilot_compute_description["os_ssh_username"],
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

    def wait_for_running(self):
        s = "UNKNOWN"
        while s != "ACTIVE":
            s = self.conn.update_server(name_or_id=self.server["id"])["status"]
            print("Server: {} Status: {}".format(self.server["id"], s))
            if s != "ACTIVE":   time.sleep(2)


    def get_node_list(self):
        nodes = []
        # for i in self.ec2_instances:
        #    nodes.append(i.private_ip_address)
        host = self.ip["floating_ip_address"]
        return [host]

    def get_id(self):
        return self.job_id

    def get_state(self):
        # all_running = all(i.state["Name"] == "running" for i in self.ec2_instances)
        state = self.conn.update_server(name_or_id=self.server["id"])["status"]
        all_running = (state == "ACTIVE")
        if all_running:
            return State.RUNNING
        else:
            return State.UNKNOWN

    def cancel(self):
        print("Delete server {}".format(self.server["id"]))
        self.conn.delete_server(name_or_id=self.server["id"], delete_ips=True)
        self.conn.delete_floating_ip(self.ip)
        self.conn.close()

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
    pass
