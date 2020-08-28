#######################
# Mac OS:
# brew install apache-spark kafka
# conda install dask distributed
# SPARK_HOME='/usr/local/Cellar/apache-spark/2.2.1/libexec/'
# Start Spark: /usr/local/Cellar/apache-spark/2.2.1/libexec/sbin/start-all.sh

import logging
import os
import time
import uuid
import pyspark
import pilot.plugins.dask.cluster
import pilot.plugins.kafka.cluster
import pilot.plugins.kinesis.cluster
import pilot.plugins.serverless.cluster
import pilot.plugins.spark.bootstrap_spark
import pilot.plugins.spark.cluster

logging.basicConfig(level=logging.DEBUG)


class PilotAPIException(Exception):
    pass


class PilotComputeDescription(dict):
    """ B{PilotComputeDescription (PCD).}

        A PilotComputeDescription is a based on the attributes defined on
        the SAGA Job Description.

        The PilotComputeDescription is used by the application to specify
        what kind of PilotJobs it requires.

        Example::
             pilot_compute_description = {
                           resource_url="fork://localhost",
                           working_directory="/tmp",
                           number_cores=1,
                           cores_per_node=1
                           type="spark"
                           }

        B{Attention}: The PilotComputeDescription is mapped 1:1 to the underlying SAGA-Python
        job description, which is used for launching the pilot that manages the Hadoop cluster.

    """

    def __init__(self):
        pass

    def __setattr__(self, attr, value):
        self[attr] = value

    def __getattr__(self, attr):
        return self[attr]

        #############################################################################


class PilotCompute(object):
    """ B{PilotCompute (PC)}.

        This is the object that is returned by the PilotComputeService when a
        new PilotCompute (aka Pilot-Job) is created based on a PilotComputeDescription.

        A Pilot-Compute in Pilot-Streaming represents an active Spark, Dask, Kafka cluster

        The PilotCompute object can be used by the application to keep track
        of PilotComputes that are active.

        A PilotCompute has state, can be queried, can be cancelled and be
        re-initialized.
    """

    def __init__(self, saga_job=None, details=None, spark_context=None, spark_sql_context=None, cluster_manager=None):
        self.saga_job = saga_job
        self.details = details
        self.spark_context = spark_context
        self.spark_sql_context = spark_sql_context
        self.cluster_manager = cluster_manager

    def cancel(self):
        """ Remove the PilotCompute from the PilotCompute Service.
        """
        if self.saga_job != None:
            self.saga_job.cancel()
        try:
            self.cluster_manager.cancel()
        except:
            pass

    def submit(self, function_name):
        self.cluster_manager.submit_compute_unit(function_name)

    def get_state(self):
        if self.saga_job != None:
            self.saga_job.get_state()

    def get_id(self):
        return self.cluster_manager.get_jobid()

    def get_details(self):
        return self.cluster_manager.get_config_data()

    def wait(self):
        self.cluster_manager.wait()

    def get_context(self, configuration=None):
        return self.cluster_manager.get_context(configuration)

    ##############################################################################
    # Non-API extension methods specific to Spark
    def get_spark_sql_context(self):
        if self.spark_sql_context == None:
            self.spark_sql_context = SQLContext(self.get_spark_context())
        return self.spark_sql_context


class PilotComputeService(object):
    """  B{PilotComputeService (PCS).}

        The PilotComputeService is responsible for creating and managing
        the PilotComputes.

        It is the application's interface to the Pilot-Manager in the
        P* Model.

    """

    def __init__(self, pjs_id=None):
        """ Create a PilotComputeService object.

            Keyword arguments:
            pjs_id -- Don't create a new, but connect to an existing (optional)
        """
        pass

    @classmethod
    def create_pilot(cls, pilotcompute_description):
        """ Add a PilotCompute to the PilotComputeService

            Keyword arguments:
            pilotcompute_description -- PilotCompute Description

            Return value:
            A PilotCompute handle
        """

        # {
        #     "resource_url":"slurm+ssh://localhost",
        #     "number_cores": 16,
        #     "cores_per_node":16,
        #     "project": "TG-MCB090174",
        #     "type":"spark"
        # }

        resource_url = "fork://localhost"
        if "resource" in pilotcompute_description:
            resource_url = pilotcompute_description["resource"]
        if resource_url.startswith("yarn"):
            p = cls.__connected_yarn_spark_cluster(pilotcompute_description)
            return p
        elif resource_url.startswith("spark"):
            print(("Connect to Spark cluster: " + str(resource_url)))
            p = cls.__connected_spark_cluster(resource_url, pilotcompute_description)
            return p
        else:
            p = cls.__start_cluster(pilotcompute_description)
            return p

    def cancel(self):
        """ Cancel the PilotComputeService.

            This also cancels all the PilotJobs that were under control of this PJS.

            Keyword arguments:
            None

            Return value:
            Result of operation
        """
        pass

    ####################################################################################################################
    # Start Cluster
    @classmethod
    def __start_cluster(self, pilotcompute_description):
        """
        Bootstraps Spark Cluster
        :param pilotcompute_description: dictionary containing detail about the spark cluster to launch
        :return: Pilot
        """
        # import commandline.main
        # spark_cluster = commandline.main.PilotStreamingCLI()

        if "resource" in pilotcompute_description:
            resource_url = pilotcompute_description["resource"]

        working_directory = "/tmp"
        if "working_directory" in pilotcompute_description:
            working_directory = pilotcompute_description["working_directory"]

        print("Working Directory: {}".format(working_directory))

        project = None
        if "project" in pilotcompute_description:
            project = pilotcompute_description["project"]

        reservation = None
        if 'reservation' in pilotcompute_description:
            reservation = pilotcompute_description["reservation"]

        queue = None
        if "queue" in pilotcompute_description:
            queue = pilotcompute_description["queue"]

        walltime = 10
        if "walltime" in pilotcompute_description:
            walltime = pilotcompute_description["walltime"]

        number_cores = 1
        if "number_cores" in pilotcompute_description:
            number_cores = int(pilotcompute_description["number_cores"])

        number_of_nodes = 1
        if "number_of_nodes" in pilotcompute_description:
            number_of_nodes = int(pilotcompute_description["number_of_nodes"])

        cores_per_node = 1
        if "cores_per_node" in pilotcompute_description:
            cores_per_node = int(pilotcompute_description["cores_per_node"])

        config_name = "default"
        if "config_name" in pilotcompute_description:
            config_name = pilotcompute_description["config_name"]

        parent = None
        if "parent" in pilotcompute_description:
            parent = pilotcompute_description["parent"]

        framework_type = None
        if "type" not in pilotcompute_description:
            raise PilotAPIException("Invalid Pilot Compute Description: type not specified")

        framework_type = pilotcompute_description["type"]

        manager = None
        if framework_type is None:
            raise PilotAPIException("Invalid Pilot Compute Description: invalid type: %s" % framework_type)
        elif framework_type == "spark":
            jobid = "spark-" + str(uuid.uuid1())
            manager = pilot.plugins.spark.cluster.Manager(jobid, working_directory)
        elif framework_type == "kafka":
            jobid = "kafka-" + str(uuid.uuid1())
            manager = pilot.plugins.kafka.cluster.Manager(jobid, working_directory)
        elif framework_type == "dask":
            jobid = "dask-" + str(uuid.uuid1())
            manager = pilot.plugins.dask.cluster.Manager(jobid, working_directory)
        elif framework_type == "kinesis":
            jobid = "kinesis-" + str(uuid.uuid1())
            manager = pilot.plugins.kinesis.cluster.Manager(jobid, working_directory)
        elif framework_type == "lambda":
            jobid = "lambda-" + str(uuid.uuid1())
            manager = pilot.plugins.serverless.cluster.Manager(jobid, working_directory)

        batch_job = manager.submit_job(
            resource_url=resource_url,
            number_of_nodes=number_of_nodes,
            number_cores=number_cores,
            cores_per_node=cores_per_node,
            queue=queue,
            walltime=walltime,
            project=project,
            reservation=reservation,
            config_name=config_name,
            extend_job_id=parent,
            pilot_compute_description=pilotcompute_description
        )

        details = manager.get_config_data()
        p = PilotCompute(batch_job, details, cluster_manager=manager)
        return p

    ###############################################################################################

    @classmethod
    def __connected_yarn_spark_cluster(self, pilotcompute_description):

        number_cores = 1
        if "number_cores" in pilotcompute_description:
            number_cores = int(pilotcompute_description["number_cores"])

        number_of_processes = 1
        if "number_of_processes" in pilotcompute_description:
            number_of_processes = int(pilotcompute_description["number_of_processes"])

        executor_memory = "1g"
        if "number_of_processes" in pilotcompute_description:
            executor_memory = pilotcompute_description["physical_memory_per_process"]

        conf = pyspark.SparkConf()
        conf.set("spark.num.executors", str(number_of_processes))
        conf.set("spark.executor.instances", str(number_of_processes))
        conf.set("spark.executor.memory", executor_memory)
        conf.set("spark.executor.cores", number_cores)
        if pilotcompute_description != None:
            for i in list(pilotcompute_description.keys()):
                if i.startswith("spark"):
                    conf.set(i, pilotcompute_description[i])
        conf.setAppName("Pilot-Spark")
        conf.setMaster("yarn-client")
        sc = pyspark.SparkContext(conf=conf)
        sqlCtx = pyspark.SQLContext(sc)
        pilot = PilotCompute(spark_context=sc, spark_sql_context=sqlCtx)
        return pilot

    @classmethod
    def __connected_spark_cluster(self, resource_url, pilot_description=None):
        conf = pyspark.SparkConf()
        conf.setAppName("Pilot-Spark")
        if pilot_description != None:
            for i in list(pilot_description.keys()):
                if i.startswith("spark"):
                    conf.set(i, pilot_description[i])
        conf.setMaster(resource_url)
        print((conf.toDebugString()))
        sc = pyspark.SparkContext(conf=conf)
        sqlCtx = pyspark.SQLContext(sc)
        pilot = PilotCompute(spark_context=sc, spark_sql_context=sqlCtx)
        return pilot

    @classmethod
    def get_spark_config_data(cls, working_directory=None):
        spark_home_path = pilot.plugins.spark.bootstrap_spark.SPARK_HOME
        if working_directory != None:
            spark_home_path = os.path.join(working_directory, os.path.basename(spark_home_path))
        master_file = os.path.join(spark_home_path, "conf/masters")
        print(master_file)
        counter = 0
        while os.path.exists(master_file) == False and counter < 600:
            print(("Looking for %s" % master_file))
            time.sleep(1)
            counter = counter + 1

        print(("Open master file: %s" % master_file))
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
