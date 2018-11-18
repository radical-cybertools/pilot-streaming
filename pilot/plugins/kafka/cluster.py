"""
Kafka Cluster Manager
"""
#import saga
import os, sys
import logging
import time
from . import bootstrap_kafka



class Manager():

    def __init__(self, jobid, working_directory):
        self.jobid = jobid
        self.working_directory = os.path.join(working_directory, jobid)
        self.myjob = None  # SAGA Job
        self.local_id = None  # Local Resource Manager ID (e.g. SLURM id)
        try:
            os.makedirs(os.path.join(self.working_directory, 'config'))
        except:
            pass


    # Kafka 1.0.x
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
            # create a job service for Futuregrid's 'india' PBS cluster
            js = saga.job.Service(resource_url)
            # describe our job
            jd = saga.job.Description()
            # resource requirements
            jd.total_cpu_count = int(number_cores)
            # environment, executable & arguments
            executable = "python"
            arguments = ["-m", "pilot.plugins.kafka.bootstrap_kafka"]
            if extend_job_id!=None:
                arguments = ["-m", "pilot.plugins.kafka.bootstrap_kafka", "-j", extend_job_id]
            logging.debug("Run %s Args: %s"%(executable, str(arguments)))
            jd.executable  = executable
            jd.arguments   = arguments
            # output options
            jd.output =  os.path.join("kafka_job.stdout")
            jd.error  = os.path.join("kafka_job.stderr")
            jd.working_directory=self.working_directory
            jd.queue=queue
            if project!=None:
                jd.project=project
            #jd.environment =
            if spmd_variation!=None:
                jd.spmd_variation=spmd_variation
            if walltime!=None:
                jd.wall_time_limit=walltime

            # create the job (state: New)
            self.myjob = js.create_job(jd)

            #print "Starting Spark bootstrap job ..."
            # run the job (submit the job to PBS)
            self.myjob.run()
            full_id = self.myjob.get_id()
            self.local_id = full_id[full_id.index("]-[")+3: len(full_id)-1]
            print("**** Job: " + str(self.local_id) + " State : %s" % (self.myjob.get_state()))
            #print "Wait for Spark Cluster to startup. File: %s" % (os.path.join(working_directory, "work/spark_started"))
            #self.print_pilot_streaming_job_id(myjob)
            return self.myjob
        except Exception as ex:
            print("An error occurred: %s" % (str(ex)))

    def wait(self):
        while True:
            state = self.myjob.get_state()
            logging.debug("**** Job: " + str(self.local_id) + " State: %s" % (state))
            if state=="Running":
                logging.debug("looking for Kafka startup state at: %s"%self.working_directory)
                if os.path.exists(os.path.join(self.working_directory, "kafka_started")):
                    break
            elif state == "Failed":
                break
            time.sleep(3)

            
    def get_jobid(self):
        return self.jobid

    def get_context(self):
        pass
        
            
    def get_config_data(self):
        self.wait()
        conf = os.path.join(self.working_directory, "config")
        print("look for configs in: " + conf)
        broker_config_dirs = [i if os.path.isdir(os.path.join(conf, i)) and i.find("broker-") >= 0 else None for i in
                              os.listdir(conf)]
        broker_config_dirs = [a for a in broker_config_dirs if a != None]
        print(str(broker_config_dirs))

        kafka_config ={}
        for broker in broker_config_dirs:
            with open(os.path.join(conf, broker, "server.properties"), "r") as config:
                print("Kafka Config: %s (%s)" % (conf, time.ctime(os.path.getmtime(conf))))
                lines = config.readlines()
                for line in lines:
                    if line.startswith("broker.id") or line.startswith("listeners") or line.startswith(
                            "zookeeper.connect"):
                        #print line.strip().replace("=", ": ")
                        line_comp = line.split("=")
                        kafka_config[line_comp[0].strip()]=line_comp[1].strip()
        print(str(kafka_config))
        details = {"master_url":kafka_config["zookeeper.connect"],
                   "details" : kafka_config}
        return details


    def print_config_data(self):
        details = self.get_config_data()
        print("Zookeeper: %s"%details["master_url"])


