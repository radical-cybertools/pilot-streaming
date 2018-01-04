"""
Dask Cluster Manager
"""

import saga, os
import logging
import time


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


    # Dask 1.19
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
            arguments = ["-m", "dask.bootstrap_dask"]
            if extend_job_id!=None:
                arguments = ["-m", "dask.bootstrap_dask", "-j", extend_job_id]
            logging.debug("Run %s Args: %s"%(executable, str(arguments)))
            jd.executable  = executable
            jd.arguments   = arguments
            # output options
            jd.output =  os.path.join("dask_job.stdout")
            jd.error  = os.path.join("dask_job.stderr")
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
            myjob = js.create_job(jd)

            #print "Starting Spark bootstrap job ..."
            # run the job (submit the job to PBS)
            self.myjob.run()
            #id = self.myjob.get_id()
            local_id = id[id.index("]-[")+3: len(id)-1]
            print "**** Job: " + str(self.local_id) + " State : %s" % (self.myjob.get_state())
            #print "Wait for Spark Cluster to startup. File: %s" % (os.path.join(working_directory, "work/spark_started"))
            #self.print_pilot_streaming_job_id(myjob)
            return myjob
        except Exception as ex:
            print "An error occurred: %s" % (str(ex))

    def wait(self):
        while True:
            state = self.myjob.get_state()
            logging.debug("**** Job: " + str(self.local_id) + " State: %s" % (state))
            if state=="Running":
                logging.debug("looking for spark startup state at: %s"%self.working_directory)
                if os.path.exists(os.path.join(self.working_directory, "spark_started")):
                    self.get_config_data(id, self.working_directory)
                    break
            elif state == "Failed":
                break
            time.sleep(3)
    
    def get_context(self):
        """Returns Dask Client for Schedueler"""
        pass
        
            
    def get_config_data(self):
        master_file = os.path.join(self.working_directory, "dask_scheduler")
        start_file = os.path.join(self.working_directory, "dask_started")
        # print master_file
        master = "localhost"
        counter = 0
        while not os.path.exists(master_file) and not os.path.exits(start_file) and counter < 600:
            time.sleep(2)
            counter = counter + 1

        with open(master_file, 'r') as f:
            master = f.read()
        details = {
            "master_url": "tcp://%s:7077" % master,
            "web_ui_url": "http://%s:8080" % master,
        }
        return details


    def print_config_data(self):
        details = self.get_config_data()
        print "Dask Scheduler: %s"%details["master_url"]

