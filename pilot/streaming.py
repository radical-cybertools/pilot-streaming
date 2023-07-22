import logging
import uuid
import pilot.plugins.dask.cluster
from pilot.exceptions.pilot_api_exception import PilotAPIException

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("StreamingAPI")

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

    def __init__(self, job=None, job_details=None, cluster_manager=None):
        self.job = job
        self.details = job_details
        self.cluster_manager = cluster_manager

    def cancel(self):
        """ Cancel the job & cluster manager."""
        try:
            if self.job is not None:
                self.job.cancel()
        except Exception as e:
            logger.error("Failed to cancel job. JobId: {}, error: {}", self.job, e)

        try:
            self.cluster_manager.cancel()
        except Exception as e:
            logger.error("Failed to cleanup Cluster Manager", e)

    def submit(self, function_name):
        self.cluster_manager.submit_compute_unit(function_name)

    def get_state(self):
        if self.job != None:
            self.job.get_state()

    def get_id(self):
        return self.cluster_manager.get_jobid()

    def get_details(self):
        return self.cluster_manager.get_config_data()

    def wait(self):
        self.cluster_manager.wait()

    def get_context(self, configuration=None):
        return self.cluster_manager.get_context(configuration)


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

        return cls.__start_cluster(pilotcompute_description)

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
        Bootstraps Cluster Manager
        :param pilotcompute_description: dictionary containing detail about the spark cluster to launch
        :return: Pilot
        """

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
        elif framework_type == "dask":
            jobid = "dask-" + str(uuid.uuid1())
            manager = pilot.plugins.dask.cluster.Manager(jobid, working_directory)

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
