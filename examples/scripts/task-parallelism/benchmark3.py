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

# Qiskit
from qiskit import QuantumCircuit, transpile, execute, Aer
from qiskit_aer import AerSimulator # for GPU
from qiskit.primitives import Estimator
from qiskit_aer.primitives import Estimator as AirEstimator
from qiskit_benchmark import run_graph, generate_data

logging.getLogger('qiskit').setLevel(logging.INFO)
logging.getLogger('qiskit.transpiler').setLevel(logging.WARN)
logging.getLogger('stevedore.extension').setLevel(logging.INFO)


RESOURCE_URL_HPC = "slurm://localhost"
WORKING_DIRECTORY = os.path.join(os.environ["PSCRATCH"], "work")

def start_pilot(num_nodes):
	pilot_compute_description_dask = {
		"resource": RESOURCE_URL_HPC,
		"working_directory": WORKING_DIRECTORY,
		"number_of_nodes": num_nodes,
		"queue": "regular",
		"walltime": 30,
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


if __name__=="__main__":
	# create a benchmark loop
	# num_qubits_arr = [20, 22, 24, 26, 28, 30, 32]
	num_qubits_arr = [34, 36, 38, 40, 42, 44, 46]
	n_entries = 1024
	results = []
	run_timestamp=datetime.datetime.now()
	RESULT_FILE= "pilot-quantum-summary-" + run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv"
	dask_pilot = None
	num_nodes = 4
		   
	dask_pilot, dask_client = start_pilot(num_nodes)

	print(dask_client.gather(dask_client.map(lambda a: a*a, range(10))))
	print(dask_client.gather(dask_client.map(lambda a: socket.gethostname(), range(10))))

	for i in range(5):
		for num_qubits in num_qubits_arr:
			try:
				start_compute = time.time()	
				circuits, observables =  generate_data(
						depth_of_recursion=1, # number of circuits and observables
						num_qubits=num_qubits,
						n_entries=n_entries, # number of circuits and observables => same as depth_of_recursion
						circuit_depth=1,
						size_of_observable=1
				) 
				circuits_observables =zip(circuits, observables)
				circuit_bag = db.from_sequence(circuits_observables)
				options = {"method": "statevector", "device":'GPU', "cuStateVec_enable": True, "shots":None}
				print(options)
				estimator = AirEstimator(backend_options=options)
				circuit_bag.map(lambda circ_obs: estimator.run(circ_obs[0], circ_obs[1]).result()).compute()
				end_compute = time.time()
				# write data to file
				result_string = "Number Nodes: {} Number Qubits: {} Number Circuits {} Compute: {}s".format(num_nodes, num_qubits, n_entries, end_compute-start_compute)
				print(result_string)
				results.append(result_string)
				with open(RESULT_FILE, "w") as f:
					f.write("\n".join(results))
					f.flush()
			except Exception as e:
				print("Experiment failed. Number Nodes: {} Number Qubits: {} Number Circuits {} Error: {}s".format(num_nodes, num_qubits, n_entries, e))
	
	time.sleep(30)
	dask_pilot.cancel()

