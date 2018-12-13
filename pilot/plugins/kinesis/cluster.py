"""
Kinesis Manager
"""

import os
import logging
import time
import boto3 
import json



class Manager():

    def __init__(self, jobid, working_directory):
        self.jobid = jobid
        self.stream_arn = None
        self.configuration = {}
        self.kinesis_client = boto3.client('kinesis', region_name='us-east-1')
        
    # Kinesis 
    def submit_job(self,
                   resource_url="kinesis://localhost",
                   number_of_nodes=None, # ignored
                   number_cores=1, # Number of shards
                   cores_per_node=None, # ignored
                   spmd_variation=None, # ignored
                   queue=None, # ignored
                   walltime=None, # ignored
                   project=None, # ignored
                   config_name=None,
                   extend_job_id=None,
                   pilotcompute_description=None
    ):
        try:
            
            response = self.kinesis_client.create_stream(
                                StreamName=self.jobid,
                                ShardCount=number_cores
                                )
            print("Created stream: %s"%self.jobid)
            self.configuration=self.kinesis_client.describe_stream(StreamName=self.jobid)['StreamDescription']
            self.stream_arn=self.configuration['StreamARN']
            self.configuration['master_url']=self.stream_arn
        except Exception as ex:
            print("An error occurred: %s" % (str(ex)))

    
    def wait(self):
        while self.kinesis_client.describe_stream(StreamName=self.jobid)['StreamDescription']['StreamStatus']!='ACTIVE':
            time.sleep(1)
    
    
    def cancel(self):
         self.kinesis_client.delete_stream(StreamName=self.jobid)
    
    
    def submit_compute_unit(function_name):
        pass
            
    def get_jobid(self):
        return self.stream_arn
    
    def get_context(self, configuration):
        return None
        
            
    def get_config_data(self):
        return self.configuration


    def print_config_data(self):
        print("Kinesis Stream Configuration: %s"%str(self.configuration))

