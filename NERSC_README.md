1. Configure [Multi-Factor Authentication (MFA) prior to login](https://docs.nersc.gov/connect/mfa/)
	
2. Configure [SSHProxy](https://docs.nersc.gov/connect/mfa/#sshproxy) for password less login and download the nersc private keys  in .ssh folder.
	
3. Enable the ssh config as below to login so Pilot can login just using ```ssh localhost``` or compute nodes like ``` ssh nid7648```

```cat ~/.ssh/config 
Host localhost perlmutter.nersc.gov nid* dtn*.nersc.gov
   IdentityFile ~/.ssh/nersc
   IdentitiesOnly yes
   ForwardAgent yes
```

4. copy the private key so dash can use the private key to start scheduler/worker on the compute nodes once provisioned. TODO: Not sure why nersc private key isn't working.
```
~/.ssh/nersc to ~/.ssh/mykey
```

6. install pilot-streaming from [branch](https://github.com/radical-cybertools/pilot-streaming/tree/support-perl). Once verified we can merge these changes.

```
git clone -b support-perl https://github.com/radical-cybertools/pilot-streaming.git
cd pilot-streaming
python setup.py install
```

8. Execute python ps-dask.py on perlmutter login node.
``` python ps-dask.py ```
