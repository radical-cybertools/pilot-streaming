"""
EC2 Cluster Manager
"""


import os
import logging
logging.getLogger("tornado.application").setLevel(logging.CRITICAL)
logging.getLogger("distributed.utils").setLevel(logging.CRITICAL)
import time
import distributed
from pilot.job.slurm import Service, Job
import boto3
import json
boto3.setup_default_session(profile_name='dev')

class Manager():

    def __init__(self, jobid, working_directory):
        self.jobid = jobid
        self.working_directory = os.path.join(working_directory, jobid)
        self.myjob = None  # SAGA Job
        self.local_id = None  # Local Resource Manager ID (e.g. SLURM id)
        self.ec2_client = boto3.resource('ec2', region_name='us-east-1')
        try:
            os.makedirs(self.working_directory)
        except:
            pass


    # Dask 1.20
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
                   pilotcompute_description=None
    ):
        try:
            print(str(pilot_compute_description))
            InstanceMarketOptions = None
            if pilotcompute_description.has_key("ec2_spot") and pilotcompute_description["ec2_spot"]=="True":
                print("Create Spot Request")
                InstanceMarketOptions = {'MarketType': 'spot',
                                            'SpotOptions': {                                                
                                                    'SpotInstanceType': 'one-time',
                                                    'BlockDurationMinutes': 180,
                                                    'InstanceInterruptionBehavior': 'terminate'
                                                    }
                                        }
            
            self.ec2_instances = ec2_client.create_instances(ImageId=pilotcompute_description["ec2_image_id"],
                                            InstanceType=pilotcompute_description["ec2_instance_type"],
                                            KeyName=pilotcompute_description["ec2_ssh_keyname"],
                                            SubnetId=pilotcompute_description["ec2_subnet_id"],
                                            SecurityGroupIds=[pilotcompute_description["ec2_security_group"]],
                                            InstanceMarketOptions = InstanceMarketOptions,
                                            TagSpecifications=[{'ResourceType': 'instance',
                                                                'Tags': [{"Key":"Name", 
                                                                          "Value":pilotcompute_description["ec2_name"]}]}],
                                            MinCount=pilotcompute_description["ec2_number_vms"], 
                                            MaxCount=pilotcompute_description["ec2_number_vms"])
            
        except Exception as ex:
            print("An error occurred: %s" % (str(ex)))

    def wait(self):
        while True:
            for instance in self.ec2_instances:
                instance.wait_until_running()
                instance.load()
                
            state = self.myjob.get_state()
            logging.debug("**** Job: " + str(self.local_id) + " State: %s" % (state))
            if state=="Running":
                logging.debug("looking for Dask startup state at: %s"%self.working_directory)
                if self.is_scheduler_started():
                    for i in range(3):
                        try:
                            #print "init distributed client"
                            c=self.get_context()
                            c.scheduler_info()
                            return
                        except IOError as e:
                            time.sleep(0.5)
            elif state == "Failed":
                break
            time.sleep(3)
            
    def cancel(self):
        c=self.get_context()
        c.run_on_scheduler(lambda dask_scheduler=None: dask_scheduler.close() & sys.exit(0))
            
    def submit_compute_unit(function_name):
        pass
    
    def get_context(self):
        """Returns Dask Client for Scheduler"""
        details=self.get_config_data()
        if details is not None:
            print("Connect to Dask: %s"%details["master_url"])
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
            
        master_host = master.split(":")[0]
        details = {
            "master_url": "tcp://%s:8786" % master_host,
            "web_ui_url": "http://%s:8787" % master_host,
        }
        return details


    def print_config_data(self):
        details = self.get_config_data()
        print("Dask Scheduler: %s"%details["master_url"])

        
    def is_scheduler_started(self):
        logging.debug("Results of scheduler startup file check: %s"%str(os.path.exists(os.path.join(self.working_directory, "dask_scheduler"))))
        return os.path.exists(os.path.join(self.working_directory, "dask_scheduler"))
