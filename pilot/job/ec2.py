#!/usr/bin/env python

import boto3
boto3.setup_default_session(profile_name='dev')
import os
import math
import uuid
import time
import traceback
import sys
import subprocess
import datetime
import json
import logging

class State:
    UNKNOWN="Unknown"
    PENDING="Pending"
    RUNNING="Running"
    FAILED="Failed"
    DONE="done"



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
    """ Plugin for Amazon EC2

        Starts VM and executes BJ agent on this VM


        Eucalyptus on FutureGrid uses a self-signed certificate, which 1) needs to be added to boto configuration
        or 2) certificate validation needs to be disabled.
    """

    def __init__(self, job_description, resource_url, pilot_compute_description):
        
        self.job_description = job_description
        print("URL: " + str(resource_url) + " Type: " + str(type(resource_url)))
        self.resource_url = resource_url
        self.pilot_compute_description = pilot_compute_description

        self.id="pilotstreaming-" + str(uuid.uuid1())
        self.subprocess_handle=None
        self.job_timestamp=datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        #self.job_output = open("pilotstreaming_agent_output_"+self.job_timestamp+".log", "w")
        #self.job_error = open("pilotstreaming_agent_output__agent_error_"+self.job_timestamp+".log", "w")
        self.ec2_client = boto3.resource('ec2', region_name='us-east-1')
        self.job_id = ""
        
    def run_ec2_instances(self):
        print(str(self.pilot_compute_description))
        InstanceMarketOptions = None
        if "ec2_spot" in self.pilot_compute_description and self.pilot_compute_description["ec2_spot"]=="True":
            print("Create Spot Request")
            InstanceMarketOptions = {'MarketType': 'spot',
                                        'SpotOptions': {                                                
                                                'SpotInstanceType': 'one-time',
                                                'InstanceInterruptionBehavior': 'terminate'
                                                }
                                    }
        self.ec2_instances = self.ec2_client.create_instances(ImageId=self.pilot_compute_description["ec2_image_id"],
                                            InstanceType=self.pilot_compute_description["ec2_instance_type"],
                                            KeyName=self.pilot_compute_description["ec2_ssh_keyname"],
                                            #SubnetId=ec2_description["ec2_subnet_id"],
                                            #SecurityGroupIds=[ec2_description["ec2_security_group"]],
                                            TagSpecifications=[{'ResourceType': 'instance',
                                                                'Tags': [{"Key":"Name", 
                                                                          "Value":self.pilot_compute_description["ec2_name"]}]}],
                                            NetworkInterfaces=[{'AssociatePublicIpAddress': True, 
                                                                'DeviceIndex': 0,
                                                                'SubnetId': self.pilot_compute_description["ec2_subnet_id"],
                                                                'Groups': [self.pilot_compute_description["ec2_security_group"]]}],
                                            InstanceMarketOptions = InstanceMarketOptions,
                                            BlockDeviceMappings=[{
                                                        'DeviceName': '/dev/xvda',
                                                        'Ebs': {'VolumeSize': 30,
                                                                'VolumeType': 'gp2'}}],
                                            MinCount=self.pilot_compute_description["number_cores"], 
                                            MaxCount=self.pilot_compute_description["number_cores"])
        
        if "type" in self.pilot_compute_description and self.pilot_compute_description["type"] == "dask":
            """TODO Move Dask specific stuff into Dask plugin"""
            self.wait_for_running()
            print("Run Dask")
            time.sleep(30)
            self.run_dask()           


    def run(self):
        """ Start VMs """
        # Submit job
        print("Run EC2 VMs")
        self.working_directory = os.getcwd()
        if "working_directory" in self.job_description:
            self.working_directory=self.job_description["working_directory"]
            print("Working Directory: %s"%self.working_directory)
            try:
                os.makedirs(self.working_directory, exist_ok=True)
            except:
                pass
        
        self.run_ec2_instances()
        self.wait_for_running()
        return self
        
    def wait_for_ssh(self, node):
        for i in range(10):
            try:
                command = "ssh -o 'StrictHostKeyChecking=no' -i {} {}@{} /bin/echo 1".format(self.pilot_compute_description["ec2_ssh_keyfile"],
                                                    self.pilot_compute_description["ec2_ssh_username"], 
                                                    node)
                print("Host: {} Command: {}".format(node, command))
                output = subprocess.check_output(command, shell=True, cwd=self.working_directory)
                print(output.decode("utf-8"))
                if output.decode("utf-8").startswith("1"):
                    print("Test successful")
                    return
            except:
                pass
            time.sleep(math.pow(2,i))

        
    
    def run_dask(self):
        """TODO Move Dask specific stuff into Dask plugin"""
        nodes = self.get_node_list()
        ## Update Mini Apps
        for i in nodes:
            self.wait_for_ssh(i)
            command = "ssh -o 'StrictHostKeyChecking=no' -i {} {}@{} pip install --upgrade git+ssh://git@github.com/radical-cybertools/streaming-miniapps.git".format(self.pilot_compute_description["ec2_ssh_keyfile"],
                                                self.pilot_compute_description["ec2_ssh_username"], 
                                                i)
            print("Host: {} Command: {}".format(i, command))
            install_process = subprocess.Popen(command, shell=True, cwd=self.working_directory)
            install_process.wait()
        
        ## Run Dask
        command = "dask-ssh --ssh-private-key %s --ssh-username %s --remote-dask-worker distributed.cli.dask_worker %s"%(self.pilot_compute_description["ec2_ssh_keyfile"],
                                 self.pilot_compute_description["ec2_ssh_username"],                                                        " ".join(nodes))
        if "cores_per_node" in self.pilot_compute_description:
            command = "dask-ssh --ssh-private-key %s --ssh-username %s --nthreads %s --remote-dask-worker distributed.cli.dask_worker %s"%(self.pilot_compute_description["ec2_ssh_keyfile"], self.pilot_compute_description["ec2_ssh_username"],  str(self.pilot_compute_description["cores_per_node"]), " ".join(nodes))
        print("Start Dask Cluster: " + command)
        #status = subprocess.call(command, shell=True)
        for i in range(3):
            self.dask_process = subprocess.Popen(command, shell=True, cwd=self.working_directory, close_fds=True)
            time.sleep(10)
            if self.dask_process.poll!=None:
                with open(os.path.join(self.working_directory, "dask_scheduler"), "w") as master_file:
                    master_file.write(nodes[0]+":8786")
                break
        

    def wait_for_running(self):
        for i in self.ec2_instances:
            i.wait_until_running()
            i.load()


    def get_node_list(self):
        nodes=[]
        for i in self.ec2_instances:
             nodes.append(i.private_ip_address)
        return nodes
    
    def get_id(self):
        return self.job_id
    
    def get_state(self):
        all_running = all(i.state["Name"]=="running" for i in self.ec2_instances)
        if all_running:
            return State.RUNNING
        else:
            return State.UNKNOWN
        

    def cancel(self):
        for i in self.ec2_instances:
            i.terminate()



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
