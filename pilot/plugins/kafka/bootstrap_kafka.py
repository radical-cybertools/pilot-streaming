#!/usr/bin/env python
""" Kafka Bootstrap Script (based on Kafka 1.0.0 release) """
import datetime
import logging
import os
import shutil
import signal
import socket
import sys
import time
import urllib.error
import urllib.request
import uuid
from optparse import OptionParser

import hostlist
import pkg_resources
from pykafka import KafkaClient

logging.basicConfig(level=logging.DEBUG)

# For automatic Download and Installation
# VERSION="0.10.1.0"
VERSION = "2.7.0"
# KAFKA_DOWNLOAD_URL = "http://www-us.apache.org/dist/kafka/" + VERSION + "/kafka_2.11-" + VERSION + ".tgz"
# KAFKA_DOWNLOAD_URL = "http://apache.mirrors.lucidnetworks.net/kafka/"+ VERSION + "/kafka_2.11-" + VERSION + ".tgz"
# KAFKA_DOWNLOAD_URL = "http://mirrors.gigenet.com/apache/kafka/"+ VERSION + "/kafka_2.11-" + VERSION + ".tgz"
#KAFKA_DOWNLOAD_URL = "http://mirrors.gigenet.com/apache/kafka/" + VERSION + "/kafka_2.13-" + VERSION + ".tgz"
KAFKA_DOWNLOAD_URL = "https://downloads.apache.org/kafka/" + VERSION + "/kafka_2.13-" + VERSION + ".tgz"
WORKING_DIRECTORY = os.path.join(os.getcwd())

# For using an existing installation
KAFKA_HOME = os.path.join(os.getcwd(), "kafka-" + VERSION)
KAFKA_CONF_DIR = os.path.join(KAFKA_HOME, "etc")

STOP = False


def handler(signum, frame):
    logging.debug("Signal catched. Stop Kafka")
    global STOP
    STOP = True
    time.sleep(10)
    os.system("killall java")
    sys.exit(0)



class KafkaBootstrap():

    def __init__(self, working_directory, kafka_home, config_name="default", extension_job_id=None):
        self.working_directory = working_directory
        self.kafka_home = kafka_home
        self.config_name = config_name
        self.jobid = "kafka-" + str(uuid.uuid1())
        self.job_working_directory = os.path.join(WORKING_DIRECTORY)
        self.job_conf_dir = os.path.join(self.job_working_directory, "config")
        self.extension_job_id = extension_job_id
        self.broker_config_files = {}
        try:
            os.makedirs(self.job_conf_dir)
            os.system("rm -rf /tmp/zookeeper*")
            os.system("rm -rf /tmp/kafka-logs*")
        except:
            pass

    def get_server_properties(self, master, hostname, broker_id):
        module = "pilot.plugins.kafka.configs." + self.config_name
        print(("Access config in module: " + module + " File: server.properties"))
        my_data = pkg_resources.resource_string(module, "server.properties").decode("utf-8")
        # print(my_data)
        # Find out external IP
        external_ip = hostname
        try:
            # check whether this host has an external ip that should be used
            external_ip = urllib.request.urlopen('https://ident.me').read().decode('utf8')
            logging.debug("External IP discovered: {}".format(external_ip))
        except:
            logging.debug("No external IP discovered.")

        # have at least 4 Kafka log directories containing broker id in config template
        my_data = my_data % (broker_id, hostname, external_ip, broker_id, broker_id, broker_id, broker_id, master)
        my_data = os.path.expandvars(my_data)
        return my_data

    def get_zookeeper_properties(self, hostname):
        module = "pilot.plugins.kafka.configs." + self.config_name
        logging.debug("Access config in module: " + module + " File: zookeeper.properties")
        my_data = pkg_resources.resource_string(module, "zookeeper.properties").decode("utf-8")
        return my_data

    #######################################################################################
    ## Get Node List from Resource Management System
    @staticmethod
    def get_pbs_allocated_nodes():
        print("Init PBS")
        pbs_node_file = os.environ.get("PBS_NODEFILE")
        if pbs_node_file == None:
            return ["localhost"]
        f = open(pbs_node_file)
        nodes = f.readlines()
        for i in nodes:
            i.strip()
        f.close()
        return list(set(nodes))

    @staticmethod
    def get_sge_allocated_nodes():
        logging.debug("Init SGE or Local")
        sge_node_file = os.environ.get("PE_HOSTFILE")
        if sge_node_file == None:
            return ["localhost"]
        f = open(sge_node_file)
        sgenodes = f.readlines()
        f.close()
        nodes = []
        for i in sgenodes:
            columns = i.split()
            try:
                for j in range(0, int(columns[1])):
                    print(("add host: " + columns[0].strip()))
                    nodes.append(columns[0] + "\n")
            except:
                pass
        nodes.reverse()
        return list(set(nodes))

    @staticmethod
    def get_slurm_allocated_nodes():
        print("Init nodefile from SLURM_NODELIST")
        hosts = os.environ.get("SLURM_NODELIST")
        if hosts == None:
            return ["localhost"]

        print("***** Hosts: " + str(hosts))
        hosts = hostlist.expand_hostlist(hosts)
        number_cpus_per_node = 1
        if os.environ.get("SLURM_CPUS_ON_NODE") != None:
            number_cpus_per_node = int(os.environ.get("SLURM_CPUS_ON_NODE"))
        freenodes = []
        for h in hosts:
            # for i in range(0, number_cpus_per_node):
            freenodes.append((h + "\n"))
        return list(set(freenodes))

    @staticmethod
    def get_nodelist_from_resourcemanager():
        if (os.environ.get("PBS_NODEFILE") != None and os.environ.get("PBS_NODEFILE") != ""):
            nodes = KafkaBootstrap.get_pbs_allocated_nodes()
        elif (os.environ.get("SLURM_NODELIST") != None):
            nodes = KafkaBootstrap.get_slurm_allocated_nodes()
        else:
            nodes = KafkaBootstrap.get_sge_allocated_nodes()
        return nodes

    #######################################################################################
    def configure_kafka(self):
        logging.debug("Kafka Instance Configuration Directory: " + self.job_conf_dir)
        nodes = self.get_nodelist_from_resourcemanager()
        logging.debug("Kafka nodes: " + str(nodes))
        master = socket.gethostname().split(".")[0]

        for idx, node in enumerate(nodes):
            path = os.path.join(self.job_conf_dir, "broker-%d" % idx)
            os.makedirs(path)
            server_properties_filename = os.path.join(path, "server.properties")
            server_properties_file = open(server_properties_filename, "w")
            server_properties_file.write(
                self.get_server_properties(master=master, hostname=node.strip(), broker_id=idx))
            server_properties_file.close()
            self.broker_config_files[node] = server_properties_filename

        zookeeper_properties_file = open(os.path.join(self.job_conf_dir, "zookeeper.properties"), "w")
        zookeeper_properties_file.write(self.get_zookeeper_properties(master))
        zookeeper_properties_file.close()

    def start_kafka(self):
        logging.debug("Start Kafka")
        os.system("killall -s 9 java")
        os.system("pkill -9 java")
        time.sleep(5)

        logging.debug("Start Zookeeper")
        start_command = os.path.join(self.kafka_home, "bin/zookeeper-server-start.sh") + " -daemon " + os.path.join(
            self.job_conf_dir, "zookeeper.properties")
        logging.debug("Execute: %s" % start_command)
        os.system(". ~/.bashrc & " + start_command)

        logging.debug("Start Kafka Cluster")
        for node in list(self.broker_config_files.keys()):
            config = self.broker_config_files[node]
            start_command = os.path.join("ssh " + node.strip() + " " + self.kafka_home, "bin/kafka-server-start.sh") + \
                            " -daemon " + config
            logging.debug("Execute: %s" % start_command)
            os.system(". ~/.bashrc & " + start_command)

        print(("Kafka started with configuration: %s" % self.job_conf_dir))

    def check_kafka(self):
        brokers = {}
        try:
            master = socket.gethostname().split(".")[0]
            client = KafkaClient(zookeeper_hosts=master + ":2181")
            brokers = client.brokers
        except:
            pass
        print("Found %d brokers: %s" % (len(list(brokers.keys())), str(brokers)))
        return brokers

    def start(self):
        self.configure_kafka()
        self.start_kafka()

    ##################################################################################################################
    # Extend cluster
    def start_kafka_extension(self):
        logging.debug("Start Kafka")
        os.system("killall -s 9 java")
        os.system("pkill -9 java")
        time.sleep(5)

        logging.debug("Start Kafka Cluster")
        for node in list(self.broker_config_files.keys()):
            config = self.broker_config_files[node]
            start_command = os.path.join("ssh " + node.strip() + " " + self.kafka_home, "bin/kafka-server-start.sh") + \
                            " -daemon " + config
            logging.debug("Execute: %s" % start_command)
            os.system(". ~/.bashrc & " + start_command)

        print(("Kafka started with configuration: %s" % self.job_conf_dir))

    def configure_kafka_extension(self):
        logging.debug("Kafka Instance Configuration Directory: " + self.job_conf_dir)
        nodes = self.get_nodelist_from_resourcemanager()
        logging.debug("Kafka nodes: " + str(nodes))
        master = self.find_parent_zookeeper()
        max_id = self.find_max_broker_id()
        for node in nodes:
            idx = max_id + 1
            path = os.path.join(self.job_conf_dir, "broker-%d" % idx)
            os.makedirs(path)
            server_properties_filename = os.path.join(path, "server.properties")
            server_properties_file = open(server_properties_filename, "w")
            server_properties_file.write(
                self.get_server_properties(master=master, hostname=node.strip(), broker_id=idx))
            server_properties_file.close()
            self.broker_config_files[node] = server_properties_filename

    def find_max_broker_id(self):
        path_to_parent_kafka_configs = os.path.join(os.getcwd(), "..", self.extension_job_id, "config")
        files = os.listdir(path_to_parent_kafka_configs)
        max_id = 0
        for i in files:
            bid = 0
            try:
                bid = int(i[-1])
            except:
                pass
            if bid > max_id:
                max_id = bid
        return max_id

    def find_parent_zookeeper(self):
        path_to_parent_spark_job = os.path.join(os.getcwd(), "..", self.extension_job_id,
                                                "config/broker-0/server.properties")
        print("Master of Parent Cluster: %s" % path_to_parent_spark_job)
        zk = None
        with open(path_to_parent_spark_job, "r") as config:
            lines = config.readlines()
            for line in lines:
                if line.startswith("zookeeper.connect="):
                    zk = line.strip().split("=")[1]
                    zk = zk.split(":")[0]

        logging.debug("Parent Zookeeper: %s" % zk)
        return zk

    def extend(self):
        self.configure_kafka_extension()
        self.start_kafka_extension()

    ##################################################################################################################
    # Stop
    def stop(self):
        self.stop_kafka()

    def stop_kafka(self):
        logging.debug("Stop Kafka")
        self.set_env()
        stop_command = os.path.join(KAFKA_HOME, "bin/kafka-server-stop.sh")
        logging.debug("Execute: %s" % stop_command)
        stop_command = os.path.join(KAFKA_HOME, "bin/zookeeper-server-stop.sh")
        logging.debug("Execute: %s" % stop_command)
        os.system(stop_command)

    ##################################################################################################################
    # Utils


#########################################################
#  main                                                 #
#########################################################
if __name__ == "__main__":

    signal.signal(signal.SIGALRM, handler)
    signal.signal(signal.SIGABRT, handler)
    signal.signal(signal.SIGQUIT, handler)
    signal.signal(signal.SIGINT, handler)

    parser = OptionParser()
    parser.add_option("-s", "--start", action="store_true", dest="start",
                      help="start Kafka", default=True)
    parser.add_option("-j", "--job", type="string", action="store", dest="jobid",
                      help="Job ID of Kafka Cluster to Extend")
    parser.add_option("-q", "--quit", action="store_false", dest="start",
                      help="terminate Hadoop")
    parser.add_option("-c", "--clean", action="store_true", dest="clean",
                      help="clean Kafka topics in Zookeeper after termination")

    parser.add_option("-n", "--config_name", action="store", type="string", dest="config_name", default="default")
    parser.add_option("-h", "--hosts", action="store", type="string", dest="config_name")

    (options, args) = parser.parse_args()
    config_name = options.config_name
    logging.debug("Bootstrap Kafka on " + socket.gethostname())

    node_list = KafkaBootstrap.get_nodelist_from_resourcemanager()
    number_nodes = len(node_list)
    print("nodes: %s" % str(node_list))


    #################################################################################################################
    # Download Kafka
    run_timestamp = datetime.datetime.now()
    performance_trace_filename = "kafka_performance_" + run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv"
    kafka_config_filename = "kafka_config_" + run_timestamp.strftime("%Y%m%d-%H%M%S")
    try:
        os.makedirs(WORKING_DIRECTORY)
    except:
        pass
    performance_trace_file = open(os.path.join(WORKING_DIRECTORY, performance_trace_filename), "a")
    start = time.time()
    # performance_trace_file.write("start_time, %.5f"%(time.time()))

    filename = os.path.basename(KAFKA_DOWNLOAD_URL)
    kafka_home = ""
    if not os.path.exists(KAFKA_HOME):
        try:
            os.makedirs(WORKING_DIRECTORY)
        except:
            pass

        download_destination = os.path.join(WORKING_DIRECTORY, filename)
        if os.path.exists(download_destination) == False:
            logging.debug("Download: %s to %s" % (KAFKA_DOWNLOAD_URL, download_destination))
            opener = urllib.request.FancyURLopener({})
            opener.retrieve(KAFKA_DOWNLOAD_URL, download_destination);
        else:
            logging.debug("Found existing Kafka binaries at: " + download_destination)
        logging.debug("Install Kafka")

        os.chdir(WORKING_DIRECTORY)
        os.system("tar -xzf %s" % filename)
        kafka_home = os.path.join(WORKING_DIRECTORY, os.path.splitext(filename)[0])
        os.environ["KAFKA_HOME"] = kafka_home

    end_download = time.time()
    performance_trace_file.write("download, %d, %.5f\n" % (number_nodes, end_download - start))
    performance_trace_file.flush()

    # initialize object for managing kafka clusters
    kafka = KafkaBootstrap(WORKING_DIRECTORY, kafka_home, config_name, options.jobid)
    if options.jobid is not None and options.jobid != "None":
        logging.debug("Extend Kafka Cluster with PS ID: %s" % options.jobid)
        kafka.extend()
        end_start = time.time()
        performance_trace_file.write("startup-extension, %d, %.5f\n" % (number_nodes, (end_start - end_download)))
        performance_trace_file.flush()
        with open("kafka_started", "w") as f:
            f.write(str(node_list))
    elif options.start:
        kafka.start()
        number_brokers = 0
        while number_brokers != number_nodes:
            brokers = kafka.check_kafka()
            number_brokers = len(list(brokers.values()))
            logging.debug("Number brokers: %d, number nodes: %d" % (number_brokers, number_nodes))
            time.sleep(1)
        end_start = time.time()
        performance_trace_file.write("startup, %d, %.5f\n" % (number_nodes, (end_start - end_download)))
        performance_trace_file.flush()
        with open("kafka_started", "w") as f:
            f.write(str(node_list))
    else:
        kafka.stop()
        if options.clean:
            directory = "/tmp/zookeeper/"
            logging.debug("delete: " + directory)
            shutil.rmtree(directory)
        sys.exit(0)

    print("Finished launching of Kafka Cluster - Sleeping now")

    while STOP == False:
        logging.debug("stop: " + str(STOP))
        time.sleep(10)

    kafka.stop()
    os.remove(os.path.join(WORKING_DIRECTORY, "kafka_started"))
    performance_trace_file.write("total_runtime, %d, %.5f\n" % (number_nodes, time.time() - start))
    performance_trace_file.flush()
    performance_trace_file.close()
