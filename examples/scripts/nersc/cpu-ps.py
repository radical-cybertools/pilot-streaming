import os
import socket

import distributed
import pennylane as qml

import pilot.streaming

RESOURCE_URL_HPC = "slurm://localhost"
WORKING_DIRECTORY = os.path.join(os.environ["HOME"], "work")

pilot_compute_description_dask = {
    "resource": RESOURCE_URL_HPC,
    "working_directory": os.path.join(os.path.expanduser("~"), "work"),
    "number_cores": 48,
    "queue": "normal",
    "walltime": 5,
    "type": "dask",
    "project": "m4408",
    "scheduler_script_commands": ["#SBATCH --constraint=cpu"]
}


def run_circuit():
    wires = 4
    layers = 1

    dev = qml.device('default.qubit', wires=wires, shots=None)

    @qml.qnode(dev)
    def circuit(parameters):
        qml.StronglyEntanglingLayers(weights=parameters, wires=range(wires))
        return [qml.expval(qml.PauliZ(i)) for i in range(wires)]

    shape = qml.StronglyEntanglingLayers.shape(n_layers=layers, n_wires=wires)
    weights = qml.numpy.random.random(size=shape)
    val = circuit(weights)
    return val


if __name__ == "__main__":
    dask_pilot = pilot.streaming.PilotComputeService.create_pilot(pilot_compute_description_dask)
    print("waiting for dask pilot to start")
    dask_pilot.wait()
    print("waiting done for dask pilot to start")
    print(dask_pilot.get_details())

    dask_client = distributed.Client(dask_pilot.get_details()['master_url'])
    dask_client.scheduler_info()

    print(dask_client.gather(dask_client.map(lambda a: a * a, range(10))))
    print(dask_client.gather(dask_client.map(lambda a: socket.gethostname(), range(10))))
    print(dask_client.gather(dask_client.map(lambda a: run_circuit(), range(10))))
    dask_pilot.cancel()
