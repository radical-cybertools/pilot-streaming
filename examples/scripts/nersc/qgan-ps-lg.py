# Library imports
import math
import socket

import distributed
import numpy as np
import pandas as pd
import pennylane as qml
# Pytorch imports
import torch
import torch.nn as nn
import torch.optim as optim
import torchvision.transforms as transforms
from torch.utils.data import Dataset
import os
from contextlib import redirect_stdout

import pilot.streaming

RESOURCE_URL_HPC = "slurm://localhost"
WORKING_DIRECTORY = os.path.join(os.environ["PSCRATCH"], "work")

pilot_compute_description_dask = {
    "resource": RESOURCE_URL_HPC,
    "working_directory": WORKING_DIRECTORY,
    "number_cores": 48,
    "queue": "normal",
    "walltime": 10,
    "type": "dask",
    "project": "m4408",
    "scheduler_script_commands": ["#SBATCH --constraint=gpu"]
}


class DigitsDataset(Dataset):
    """Pytorch dataloader for the Optical Recognition of Handwritten Digits Data Set"""

    def __init__(self, csv_file, label=0, transform=None):
        """
        Args:
            csv_file (string): Path to the csv file with annotations.
            root_dir (string): Directory with all the images.
            transform (callable, optional): Optional transform to be applied
                on a sample.
        """
        self.csv_file = csv_file
        self.transform = transform
        self.df = self.filter_by_label(label)

    def filter_by_label(self, label):
        # Use pandas to return a dataframe of only zeros
        df = pd.read_csv(self.csv_file)
        df = df.loc[df.iloc[:, -1] == label]
        return df

    def __len__(self):
        return len(self.df)

    def __getitem__(self, idx):
        if torch.is_tensor(idx):
            idx = idx.tolist()

        image = self.df.iloc[idx, :-1] / 16
        image = np.array(image)
        image = image.astype(np.float32).reshape(8, 8)

        if self.transform:
            image = self.transform(image)

        # Return image and label
        return image, 0


######################################################################
# Next we define some variables and create the dataloader instance.
#

image_size = 8  # Height / width of the square images
batch_size = 1

transform = transforms.Compose([transforms.ToTensor()])
dataset = DigitsDataset(csv_file="quantum_gans/optdigits.tra", transform=transform)
dataloader = torch.utils.data.DataLoader(
    dataset, batch_size=batch_size, shuffle=True, drop_last=True
)


class Discriminator(nn.Module):
    """Fully connected classical discriminator"""

    def __init__(self):
        super().__init__()

        self.model = nn.Sequential(
            # Inputs to first hidden layer (num_input_features -> 64)
            nn.Linear(image_size * image_size, 64),
            nn.ReLU(),
            # First hidden layer (64 -> 16)
            nn.Linear(64, 16),
            nn.ReLU(),
            # Second hidden layer (16 -> output)
            nn.Linear(16, 1),
            nn.Sigmoid(),
        )

    def forward(self, x):
        return self.model(x)


# Quantum variables
n_qubits = 5  # Total number of qubits / N
n_a_qubits = 1  # Number of ancillary qubits / N_A
q_depth = 6  # Depth of the parameterised quantum circuit / D
n_generators = 4  # Number of subgenerators for the patch method / N_G


def run_training():
    with open('%s/%s' % (WORKING_DIRECTORY, 'out.txt'), 'w') as f:
        with redirect_stdout(f):
            # Quantum simulator
            dev = qml.device("lightning.gpu", wires=n_qubits)
            # Enable CUDA device if available
            device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

            ######################################################################
            # Next, we define the quantum circuit and measurement process described above.
            @qml.qnode(dev, interface="torch", diff_method="parameter-shift")
            def quantum_circuit(noise, weights):
                weights = weights.reshape(q_depth, n_qubits)

                # Initialise latent vectors
                for i in range(n_qubits):
                    qml.RY(noise[i], wires=i)

                # Repeated layer
                for i in range(q_depth):
                    # Parameterised layer
                    for y in range(n_qubits):
                        qml.RY(weights[i][y], wires=y)

                    # Control Z gates
                    for y in range(n_qubits - 1):
                        qml.CZ(wires=[y, y + 1])

                return qml.probs(wires=list(range(n_qubits)))

            # For further info on how the non-linear transform is implemented in Pennylane
            # https://discuss.pennylane.ai/t/ancillary-subsystem-measurement-then-trace-out/1532
            def partial_measure(noise, weights):
                # Non-linear Transform
                probs = quantum_circuit(noise, weights)
                probsgiven0 = probs[: (2 ** (n_qubits - n_a_qubits))]
                probsgiven0 /= torch.sum(probs)

                # Post-Processing
                probsgiven = probsgiven0 / torch.max(probsgiven0)
                return probsgiven

            class PatchQuantumGenerator(nn.Module):
                """Quantum generator class for the patch method"""

                def __init__(self, n_generators, q_delta=1):
                    """
                    Args:
                        n_generators (int): Number of sub-generators to be used in the patch method.
                        q_delta (float, optional): Spread of the random distribution for parameter initialisation.
                    """

                    super().__init__()

                    self.q_params = nn.ParameterList(
                        [
                            nn.Parameter(q_delta * torch.rand(q_depth * n_qubits), requires_grad=True)
                            for _ in range(n_generators)
                        ]
                    )
                    self.n_generators = n_generators

                def forward(self, x):
                    # Size of each sub-generator output
                    patch_size = 2 ** (n_qubits - n_a_qubits)

                    # Create a Tensor to 'catch' a batch of images from the for loop. x.size(0) is the batch size.
                    images = torch.Tensor(x.size(0), 0).to(device)

                    # Iterate over all sub-generators
                    for params in self.q_params:

                        # Create a Tensor to 'catch' a batch of the patches from a single sub-generator
                        patches = torch.Tensor(0, patch_size).to(device)
                        for elem in x:
                            q_out = partial_measure(elem, params).float().unsqueeze(0)
                            patches = torch.cat((patches, q_out))

                        # Each batch of patches is concatenated with each other to create a batch of images
                        images = torch.cat((images, patches), 1)

                    return images

            lrG = 0.3  # Learning rate for the generator
            lrD = 0.01  # Learning rate for the discriminator
            num_iter = 500  # Number of training iterations

            num_gpus = torch.cuda.device_count()
            print("Number of GPUS %s on the host %s" % (num_gpus, socket.gethostname()))

            # # Wrap the models with DataParallel
            discriminator = nn.DataParallel(Discriminator().to(device), device_ids=range(num_gpus))
            generator = nn.DataParallel(PatchQuantumGenerator(n_generators).to(device), device_ids=range(num_gpus))

            # Binary cross entropy
            criterion = nn.BCELoss()

            # Optimisers
            optD = optim.SGD(discriminator.parameters(), lr=lrD)
            optG = optim.SGD(generator.parameters(), lr=lrG)

            real_labels = torch.full((batch_size,), 1.0, dtype=torch.float, device=device)
            fake_labels = torch.full((batch_size,), 0.0, dtype=torch.float, device=device)

            # Fixed noise allows us to visually track the generated images throughout training
            fixed_noise = torch.rand(8, n_qubits, device=device) * math.pi / 2

            # Iteration counter
            counter = 0

            # Collect images for plotting later
            results = []

            while True:
                for i, (data, _) in enumerate(dataloader):

                    # Data for training the discriminator
                    data = data.reshape(-1, image_size * image_size)
                    real_data = data.to(device)

                    # Noise follwing a uniform distribution in range [0,pi/2)
                    noise = torch.rand(batch_size, n_qubits, device=device) * math.pi / 2
                    fake_data = generator(noise)

                    # Training the discriminator
                    discriminator.zero_grad()
                    outD_real = discriminator(real_data).view(-1)
                    outD_fake = discriminator(fake_data.detach()).view(-1)

                    errD_real = criterion(outD_real, real_labels)
                    errD_fake = criterion(outD_fake, fake_labels)
                    # Propagate gradients
                    errD_real.backward()
                    errD_fake.backward()

                    errD = errD_real + errD_fake
                    optD.step()

                    # Training the generator
                    generator.zero_grad()
                    outD_fake = discriminator(fake_data).view(-1)
                    errG = criterion(outD_fake, real_labels)
                    errG.backward()
                    optG.step()

                    counter += 1

                    # Show loss values
                    if counter % 10 == 0:
                        print(f'Iteration: {counter}, Discriminator Loss: {errD:0.3f}, Generator Loss: {errG:0.3f}')
                        test_images = generator(fixed_noise).view(8, 1, image_size, image_size).cpu().detach()

                        # Save images every 50 iterations
                        if counter % 50 == 0:
                            results.append(test_images)

                    if counter == num_iter:
                        break
                if counter == num_iter:
                    break


if __name__ == "__main__":
    dask_pilot, dask_client = None, None
    try:
        dask_pilot = pilot.streaming.PilotComputeService.create_pilot(pilot_compute_description_dask)
        print("waiting for dask pilot to start")
        dask_pilot.wait()
        print("waiting done for dask pilot to start")
        print(dask_pilot.get_details())

        dask_client = distributed.Client(dask_pilot.get_details()['master_url'])
        dask_client.scheduler_info()

        print(dask_client.gather(dask_client.map(lambda a: a * a, range(10))))
        print(dask_client.gather(dask_client.map(lambda a: socket.gethostname(), range(10))))

        training_future = dask_client.submit(run_training)
        dask_client.gather([training_future])
        print("Done with training...")

    finally:
        if dask_client:
            dask_client.close()

        if dask_pilot:
            dask_pilot.cancel()