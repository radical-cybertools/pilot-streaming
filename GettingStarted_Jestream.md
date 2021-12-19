# Getting started with Streaming Mini-Apps

12/19/2021

* Start Jetstream Instance with ubuntu 20.04 image: <https://use.jetstream-cloud.org/application/projects/>

* Secure VM:
  * setup firewall, e.g. ufw: <https://wiki.ubuntu.com/UncomplicatedFirewall>
  * secure all processes and daemons that are permanently running (e.g., set strong password, enable https and encryption)

* Update software on VM:

        sudo apt-get upgrade
        sudo apt-get update

        Make sure all security packages are upgraded!

* Install packages in particular Java:
  
        apt install openjdk-11-jre-headless


* Install Anaconda:
  
        wget https://repo.anaconda.com/archive/Anaconda3-2021.11-Linux-x86_64.sh
        bash Anaconda3-2021.11-Linux-x86_64.sh

* Install conda dependencies (see README in Repos):

    conda install paramiko numpy pandas dask distributed
    conda install -c conda-forge boto3 python-openstackclient pykafka pyspark dask distributed python-confluent-kafka pexpect redis-py  python-confluent-kafka pexpect redis-py scikit-learn feather-format
    pip install hostlist

* Active password-less ssh login:

        $ ssh-keygen -t rsa # select no password
        $ cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
        $ chmod 700 ~/.ssh/authorized_keys

        Test
        $ ssh localhost
        $ ssh js-129-114-17-61.jetstream-cloud.org
        

* Install Pilot-Streaming:

  * Go to source directory of pilot-streaming:
    
        $ git clone https://github.com/radical-cybertools/pilot-streaming
        $ cd pilot-streaming
        $ pip install --upgrade .


* Using the example notebook to use Pilot-Streaming to run Kafka: <https://github.com/radical-cybertools/pilot-streaming/blob/master/examples/edge_examples_jetstream.ipynb>

* Consider using an editor that supports remote editing, e.g. VS Code with Remote - SSH extension <https://code.visualstudio.com/docs/remote/ssh>





