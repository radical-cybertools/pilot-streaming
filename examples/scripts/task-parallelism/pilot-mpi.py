from mpi4py import MPI
import pennylane as qml
from pennylane import numpy as np
from timeit import default_timer as timer
import subprocess





import os, time, sys
sys.path.insert(0, os.path.abspath('../../..'))
import socket
import getpass
import datetime

# Python Dask and Data stack
import numpy as np
import pandas as pd
import distributed, dask
import dask.dataframe as dd
import dask.array as da
import dask.bag as db

# Pilot-Quantum
import pilot.streaming

import logging
logging.basicConfig(level=logging.WARNING)


RESOURCE_URL_HPC = "slurm://localhost"
WORKING_DIRECTORY = os.path.join(os.environ["PSCRATCH"], "work")

def start_pilot(num_nodes):
    pilot_compute_description_dask = {
        "resource": RESOURCE_URL_HPC,
        "working_directory": WORKING_DIRECTORY,
        "number_of_nodes": num_nodes,
        "queue": "regular",
        "walltime": 5,
        "type": "dask",
        "project": "m4408",
        "os_ssh_keyfile": "~/.ssh/nersc",
        "scheduler_script_commands": ["#SBATCH --constraint=gpu", "#SBATCH --ntasks-per-node=4", "#SBATCH -c 32","#SBATCH --gpus-per-task=1","#SBATCH --gpu-bind=none"]
    }
    dask_pilot = pilot.streaming.PilotComputeService.create_pilot(pilot_compute_description_dask)
    dask_pilot.wait()
    dask_pilot.get_details()
    dask_client  = distributed.Client(dask_pilot.get_details()['master_url'])
    return dask_pilot, dask_client



def execute_shell_command(num_gpus):
    shell_script="mpirun -np " + str(num_gpus) + " python /global/homes/p/prmantha/pilot-streaming/examples/scripts/task-parallelism/testmpi.py"

    try:
        # Run the shell command
        result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # Check for successful execution (return code 0)
        if result.returncode == 0:
            print("Command executed successfully.")
            print("Output:", result.stdout)
        else:
            print("Error executing the command.")
            print("Error:", result.stderr)

    except Exception as e:
        print("An error occurred:", str(e))



if __name__=="__main__":
    # create a benchmark loop
    # num_qubits_arr = [20, 22, 24, 26, 28, 30, 32]
    results = []
    run_timestamp=datetime.datetime.now()
    RESULT_FILE= "pilot-quantum-summary-" + run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv"
    dask_pilot = None
    num_nodes = 4
    num_qubits = 33
    num_gpus = 16

           
    dask_pilot, dask_client = start_pilot(num_nodes)

    print(dask_client.gather(dask_client.map(lambda a: a*a, range(10))))
    print(dask_client.gather(dask_client.map(lambda a: socket.gethostname(), range(10))))


    try:
        circuits_observables = [num_gpus]
        circuit_bag = db.from_sequence(circuits_observables)
        start_compute = time.time()         
        circuit_bag.map(lambda script_params: execute_shell_command(script_params)).compute()
        end_compute = time.time()
        # write data to file
        result_string = "Number Nodes: {} Number Qubits: {} Number GPUS {} Compute: {}s".format(num_nodes, num_qubits, num_gpus, end_compute-start_compute)
        print(result_string)
        results.append(result_string)
        with open(RESULT_FILE, "w") as f:
            f.write("\n".join(results))
            f.flush()
    except Exception as e:
        print("Experiment failed. Number Nodes: {} Number Qubits: {} Number GPUS {} Error: {}s".format(num_nodes, num_qubits, num_gpus, e))
    
    time.sleep(30)
    dask_pilot.cancel()









