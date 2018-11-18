"""
Dask Cluster Manager
"""


import os
import logging
logging.getLogger("tornado.application").setLevel(logging.CRITICAL)
logging.getLogger("distributed.utils").setLevel(logging.CRITICAL)
import time
import distributed
from pilot.job.slurm import Service, Job

class Manager():

    def __init__(self, jobid, working_directory):
        self.jobid = jobid
        self.working_directory = os.path.join(working_directory, jobid)
        self.myjob = None  # SAGA Job
        self.local_id = None  # Local Resource Manager ID (e.g. SLURM id)
        try:
            os.makedirs(self.working_directory)
        except:
            pass


    # Dask 1.20
    def submit_job(self,
                   resource_url="fork://localhost",
                   number_cores=1,
                   cores_per_node=1,
                   spmd_variation=None,
                   queue=None,
                   walltime=None,
                   project=None,
                   config_name="default",
                   extend_job_id=None,
                   pilotcompute_description=None
    ):
        try:
            # create a job service for SLURM LRMS
            js = Service(resource_url)
            
            # environment, executable & arguments
            executable = "python"
            arguments = ["-m", "pilot.plugins.dask.bootstrap_dask"]
            if extend_job_id!=None:
                arguments = ["-m", "pilot.plugins.dask.bootstrap_dask", "-j", extend_job_id]
            logging.debug("Run %s Args: %s"%(executable, str(arguments)))
            
            jd ={
                "executable": executable,
                "arguments": arguments,
                "working_directory": self.working_directory,
                "output": "dask_job_%s.stdout"%self.jobid,
                "error": "dask_job_%s.stderr"%self.jobid,
                "number_cores": number_cores,
                "cores_per_node": cores_per_node,
                "project": project,
                "queue": queue,
                "walltime": walltime,
            }
            self.myjob = js.create_job(jd)
            self.myjob.run()
            self.local_id = self.myjob.get_id()
            print("**** Job: " + str(self.local_id) + " State : %s" % (self.myjob.get_state()))
            return self.myjob
        except Exception as ex:
            print("An error occurred: %s" % (str(ex)))

    def wait(self):
        while True:
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
            time.sleep(1)
    
    def get_context(self):
        """Returns Dask Client for Schedueler"""
        details=self.get_config_data()
        client = distributed.Client(details["master_url"])
        return client
        
        
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
