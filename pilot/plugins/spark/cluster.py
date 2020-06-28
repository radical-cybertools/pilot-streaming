"""
Spark Cluster Manager
"""

#import saga
import os
import logging
import time
import pilot.plugins.spark.bootstrap_spark
import pyspark

from pilot.job.slurm import Service, Job


class Manager():

    def __init__(self, jobid, working_directory):
        self.jobid = jobid
        self.working_directory = os.path.join(working_directory, jobid)
        self.myjob = None # SAGA Job
        self.local_id = None # Local Resource Manager ID (e.g. SLURM id)
        try:
            os.makedirs(self.working_directory)
        except:
            pass
        self.sc=None

    # Spark 2.x
    def submit_job(self,
                   resource_url="fork://localhost",
                   number_of_nodes=1,
                   number_cores=1, # Not used at the moment
                   cores_per_node=1,
                   spmd_variation=None,
                   queue=None,
                   walltime=None,
                   project=None,
                   config_name="default",
                   reservation = None,
                   extend_job_id=None,
                   pilot_compute_description=None
    ):
        try:
            # create a job service 
            js = Service(resource_url)
            
            executable = "python"
            arguments = ["-m", "pilot.plugins.spark.bootstrap_spark"]
            if extend_job_id!=None:
                arguments = ["-m", "pilot.plugins.spark.bootstrap_spark", "-j", extend_job_id]
            logging.debug("Run %s Args: %s"%(executable, str(arguments)))

            jd ={
                "executable": executable,
                "arguments": arguments,
                "working_directory": self.working_directory,
                "output": "spark_job_%s.stdout"%self.jobid,
                "error": "spark_job_%s.stderr"%self.jobid,
                "number_of_nodes": number_of_nodes,
                "cores_per_node": cores_per_node,
                "project": project,
                "reservation": reservation,
                "queue": queue,
                "walltime": walltime,
            }
            self.myjob = js.create_job(jd)
            self.myjob.run()
            logging.debug("Job State: " + self.myjob.get_state())
            self.local_id = self.myjob.get_id() #id[id.index("]-[")+3: len(id)-1]
            print("**** Job: " + str(self.local_id) + " State : %s" % (self.myjob.get_state()))
            #self.print_pilot_streaming_job_id(myjob)
            return self.myjob
        except Exception as ex:
            print("An error occurred: %s" % (str(ex)))

    def wait(self):
        while True:
            state = self.myjob.get_state()
            logging.debug("**** Job: " + str(self.local_id) + " State: %s" % (state))
            if state=="Running":
                logging.debug("looking for spark startup state at: %s"%self.working_directory)
                if os.path.exists(os.path.join(self.working_directory, "spark_started")):
                    self.get_config_data()
                    break
            elif state == "Failed":
                break
            time.sleep(3)
            
            
    def submit_compute_unit(function_name):
        pass
        
                    
    def get_jobid(self):
        return self.jobid
    
    def get_context(self, configuration):
        if self.sc is None:
            logging.debug("Init Spark Context with Configuration: " + str(configuration))
            self.sc = pyspark.SparkContext(master=self.get_config_data()["master_url"], appName=self.jobid, conf=configuration)
        return self.sc        
        
            
    def get_config_data(self):
        spark_home_path= pilot.plugins.spark.bootstrap_spark.SPARK_HOME
        working_directory=self.working_directory
        if working_directory != None:
            spark_home_path = os.path.join(working_directory, os.path.basename(spark_home_path))
        master_file = os.path.join(spark_home_path, "conf/masters")
        #print master_file
        counter = 0
        while os.path.exists(master_file) == False and counter < 600:
            logging.debug("Looking for %s" % master_file)
            time.sleep(1)
            counter = counter + 1

        logging.debug("Open master file: %s" % master_file)
        with open(master_file, 'r') as f:
            master = f.read().strip()
        f.closed
        print(("Create Spark Context for URL: %s" % ("spark://%s:7077" % master)))
        details = {
            "spark_home": spark_home_path,
            "master_url": "spark://%s:7077" % master,
            "web_ui_url": "http://%s:8080" % master,
        }
        return details


    def print_config_data(self):
        spark_home_path= bootstrap_spark.SPARK_HOME
        # search for spark_home:
        base_work_dir = os.path.join(self.working_directory)
        spark_home=''.join([i.strip() if os.path.isdir(os.path.join(base_work_dir, i)) and i.find("spark")>=0 else '' for i in os.listdir(base_work_dir)])
        spark_home_path=os.path.join(self.working_directory, os.path.basename(spark_home_path))
        master_file=os.path.join(spark_home_path, "conf/masters")
        #print master_file
        counter = 0
        while os.path.exists(master_file)==False and counter <600:
            time.sleep(1)
            counter = counter + 1

        with open(master_file, 'r') as f:
            master = f.read()
        print("SPARK installation directory: %s"%spark_home_path)
        print("(please allow some time until the SPARK cluster is completely initialized)")
        print("export PATH=%s/bin:$PATH"%(spark_home_path))
        print("Spark Web URL: http://" + master + ":8080")
        print("Spark Submit endpoint: spark://" + master + ":7077")

