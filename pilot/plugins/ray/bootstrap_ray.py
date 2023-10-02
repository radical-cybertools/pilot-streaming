#!/usr/bin/env python
""" Ray Bootstrap Script (based on Ray 2.7 release) """
import os, sys
import pdb
import random
import urllib.request, urllib.parse, urllib.error
import subprocess
import logging
import uuid
import shutil
import time
import signal
import socket
import hostlist
from optparse import OptionParser
import pkg_resources
import datetime
import ray
import getpass

from pilot.util.ssh_utils import execute_ssh_command, execute_ssh_command_as_daemon


logging.basicConfig(level=logging.DEBUG)

WORKING_DIRECTORY = os.path.join(os.getcwd())
try:
    os.makedirs(WORKING_DIRECTORY)
except:
    pass


STOP=False

def handler(signum, frame):
    logging.debug("Signal catched. Stop Ray")
    global STOP
    STOP=True
    
    

class RayBootstrap():


    def __init__(self, working_directory, ray_home, config_name="default", extension_job_id=None, cores_per_node=1, ip_head_node=None):
        self.working_directory=working_directory
        self.ray_home=ray_home
        self.config_name=config_name
        self.jobid = "ray-"+str(uuid.uuid1())
        self.job_timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        self.job_working_directory = os.path.join(WORKING_DIRECTORY) 
        self.job_conf_dir = os.path.join(self.job_working_directory, "config")
        self.job_output = open(self.job_timestamp + "_ray_pilotstreaming_agent_output.log", "w")
        self.job_error = open(self.job_timestamp + "_ray_pilotstreaming_agent_error.log", "w")
        self.nodes = []
        self.master = ""
        self.ray_process = None
        self.extension_job_id = extension_job_id
        self.cores_per_node=cores_per_node
        self.ssh_key = "~/.ssh/mykey" #perlmutter - TODO: Should make it as input parameter from the pilot job description.
        self.ray_memory_limit=92e9    #Stampede
        self.ip_head_node=ip_head_node
        #self.ray_memory_limit=110e9 #Wrangler
        try:
            os.makedirs(self.job_conf_dir)
        except:
            pass


    
    def get_ray_properties(self, master, hostname, broker_id):
        module = "ray.configs." + self.config_name
        print(("Access config in module: " + module + " File: ray.properties"))
        my_data = pkg_resources.resource_string(module, "ray.properties")
        #my_data = my_data%(broker_id, hostname, hostname, master)
        #my_data = os.path.expandvars(my_data)
        #return my_data


    ############################################################################
    ## Get Node List from Resource Management System
    def get_pbs_allocated_nodes(self):
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

    def get_sge_allocated_nodes(self):
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
                    nodes.append(columns[0]+"\n")
            except:
                    pass
        nodes.reverse()
        return list(set(nodes))
    
    def get_slurm_allocated_nodes(self):
        print("Init nodefile from SLURM_NODELIST")
        hosts = os.environ.get("SLURM_NODELIST") 
        if hosts == None:
            return ["localhost"]

        print("***** Hosts: " + str(hosts)) 
        hosts=hostlist.expand_hostlist(hosts)
        number_cpus_per_node = 1
        if os.environ.get("SLURM_CPUS_ON_NODE")!=None:
            number_cpus_per_node=int(os.environ.get("SLURM_CPUS_ON_NODE"))
        freenodes = []
        for h in hosts:
            #for i in range(0, number_cpus_per_node):
            freenodes.append((h + "\n"))
        return list(set(freenodes))

    
    def get_nodelist_from_resourcemanager(self):
        if (os.environ.get("PBS_NODEFILE") != None and os.environ.get("PBS_NODEFILE") != ""):
            nodes = RayBootstrap.get_pbs_allocated_nodes()
        elif (os.environ.get("SLURM_NODELIST") != None):
            nodes = RayBootstrap.get_slurm_allocated_nodes()
        elif (os.environ.get("PE_HOSTFILE") != None):
            nodes = RayBootstrap.get_sge_allocated_nodes()        
        else:
            if self.ip_head_node==None:
                hostname = socket.gethostname()
                ip_address = socket.gethostbyname(hostname)
            else:
                ip_address=self.ip_head_node
            nodes = [ip_address]
        nodes =[i.strip() for i in nodes] # remove white spaces from host names
        return nodes


    ############################################################################
    def configure_ray(self):
        logging.debug("Ray Instance Configuration Directory: " + self.job_conf_dir)
        self.nodes = self.get_nodelist_from_resourcemanager()
        logging.debug("Ray nodes: " + str(self.nodes))
                  

    def start_ray(self):
        logging.debug("Start Ray Head Node")
        
        #create random port_number
        #port_number = random.randint(1025, 65535)
        #self.master = "{}:{}".format(self.nodes[0], port_number) #socket.gethostname().split(".")[0]

        if ray.is_initialized():
            logging.debug("Ray is already initialized. Shutdown Ray")
            ray.shutdown()
            time.sleep(5)

        #os.environ["RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER"] = "1"        
        # ray_client = ray.init(address=self.nodes[0], 
        #                       _node_ip_address=self.nodes[0],  
        #                       dashboard_host=self.nodes[0], num_cpus=0, num_gpus=0)
        
        cmd = "conda activate pilot-quantum; export RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER=1; ray start --head --num-cpus=%d "%(self.cores_per_node)
        print("Start Ray Head Node with command: %s"%(cmd))
        result=execute_ssh_command(host=self.nodes[0], 
                                   user=getpass.getuser(),              command=cmd, arguments=None,
                                   working_directory=self.working_directory,
                                   job_output=self.job_output, job_error=self.job_error)

        ray_client = ray.init(address=self.nodes[0])
        ray_address = ray_client.address_info["address"]
        print("Ray Head Node started at {}. Start workers now".format(ray_client.address_info))
        
        with open(os.path.join(WORKING_DIRECTORY, "ray_scheduler"), "w") as master_file:
            master_file.write(self.master)

        for i in self.nodes:
            #execute_ssh_command(i, "killall -9 ray")
            command = "export RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER=1; ray start --address %s --num-cpus=%d "%(ray_address, self.cores_per_node)
            
            result=execute_ssh_command(host=i, 
                                       user=getpass.getuser(),              command=command, arguments=None,
                                       working_directory=self.working_directory,
                                       job_output=self.job_output, job_error=self.job_error)

        #ray.shutdown()
        #time.sleep(5)
        
        
        print("Ray started.")


    def check_ray(self):
        try:
            pass
        except:
            pass
        return None
        
    def stop_ray(self):
        logging.debug("Stop Ray")
        self.ray.stop()
        

    def start(self):
        self.configure_ray()
        self.start_ray()
        
    ############################################################################
    # Extension

    def extend(self):
        self.configure_ray_extension()
        self.start_ray_extension()
    
    def start_ray_extension(self):
        logging.debug("Ray Cluster Scaling NOT SUPPORTED")
        pass
    
    def configure_ray_extension(self):
        logging.debug("Ray Cluster Scaling NOT SUPPORTED")

        
    def stop(self):
        self.stop_ray()
    


#########################################################
#  main                                                 #
#########################################################
if __name__ == "__main__" :
    

    signal.signal(signal.SIGALRM, handler)
    signal.signal(signal.SIGABRT, handler)
    signal.signal(signal.SIGQUIT, handler)
    signal.signal(signal.SIGINT, handler)

    parser = OptionParser()
    parser.add_option("-s", "--start", action="store_true", dest="start",
                  help="start Ray", default=True)
    parser.add_option("-q", "--quit", action="store_false", dest="start",
                  help="terminate Ray")
    parser.add_option("-j", "--job", type="string", action="store", dest="jobid",
                      help="Job ID of Ray Cluster to Extend")
    parser.add_option("-c", "--clean", action="store_true", dest="clean",
                  help="clean Ray")
    parser.add_option("-p", "--cores-per-node", type="string", action="store", dest="cores_per_node", default=1, help="Core Per Node")

    parser.add_option("-i", "--ip-head-node", type="string", action="store", dest="ip_head_node", default=None, help="IP Address to bind Ray daemons to on head bode")


    parser.add_option("-n", "--config_name", action="store", type="string", dest="config_name", default="default")
    
    # Parse Option from commandline arguments
    (options, args) = parser.parse_args()
    config_name=options.config_name
    logging.debug("Check Ray Installation on " + socket.gethostname())
    try:
        import ray
    except:
        print("No Ray found. Please install Ray!")
  
    # Initialize object for managing Ray clusters
    ray_cluster = RayBootstrap(WORKING_DIRECTORY, None, None, options.jobid, options.cores_per_node, options.ip_head_node)
    run_timestamp = ray_cluster.job_timestamp

    node_list = ray_cluster.get_nodelist_from_resourcemanager()
    number_nodes = len(node_list)
    print("nodes: %s"%str(node_list))
    performance_trace_filename = "ray_performance_" + run_timestamp + ".csv"
    ray_config_filename = "ray_config_" + run_timestamp
    performance_trace_file = open(os.path.join(WORKING_DIRECTORY,   
                                               performance_trace_filename), "a")
    start = time.time()
    #performance_trace_file.write("start_time, %.5f"%(time.time()))
 
    if options.jobid is not None and options.jobid != "None":
        logging.debug("Extend Ray Cluster with PS ID: %s" % options.jobid)
        logging.error("Ray Cluster Scaling NOT SUPPORTED")
        #ray_cluster.extend()
    elif options.start:
        ray_cluster.start()
        number_brokers=0
        while number_brokers!=number_nodes:
            ray_nodes=ray_cluster.check_ray()
            logging.debug("Ray Info: %s"%(ray_nodes))
            time.sleep(1)
        end_start = time.time()
        performance_trace_file.write("startup, %d, %.5f\n"%(number_nodes, (end_start-start)))
        performance_trace_file.flush()
        with open("ray_started", "w") as f:
            f.write(str(node_list))

    else:
        ray_cluster.stop()
        if options.clean:
            pass
            # directory = "/tmp/zookeeper/"
            # logging.debug("delete: " + directory)
            # shutil.rmtree(directory)
        sys.exit(0)
    
    print("Finished launching of Ray Cluster - Sleeping now")

    while not STOP:
        logging.debug("stop: " + str(STOP))
        time.sleep(10)
            
    ray_cluster.stop()
    os.remove(os.path.join(WORKING_DIRECTORY, "ray_started"))
    performance_trace_file.write("total_runtime, %d, %.5f\n"%(number_nodes, time.time()-start))
    performance_trace_file.flush()
    performance_trace_file.close()
        
        
    
    
    
