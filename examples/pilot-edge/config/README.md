# Notes on Preparing Image


based on Ubuntu 20.04

1. Update Ubuntu
   
        sudo apt-get update
        sudo apt-get upgrade
        sudo apt-get install openjdk-11-jdk redis-server
         
2. Set SSH Login

        ssh-keygen -t ecdsa
        cat ~/.ssh/id_ecdsa.pub >> ~/.ssh/authorized_keys
        rm ~/.ssh/known_hosts

2. Download Anaconda

        wget https://repo.anaconda.com/archive/Anaconda3-2020.11-Linux-x86_64.sh
        bash Anaconda3-2020.11-Linux-x86_64.sh

4. Install packages
        conda upgrade anaconda
        conda install paramiko  
        conda install -c conda-forge dask distributed boto3 python-openstackclient pykafka pyspark  python-confluent-kafka pexpect redis-py  
        pip install hostlist


   For experimental purposes
         
         conda install tensorflow
         pip install --upgrade pyod
         pip install --upgrade git+ssh://git@github.com/radical-cybertools/pilot-streaming.git (Github token is required)
         pip install --upgrade git+ssh://git@github.com/radical-cybertools/streaming-miniapps.git

        



