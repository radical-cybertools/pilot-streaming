1. Configured Multi-Factor Authentication (MFA) prior to login.
	https://docs.nersc.gov/connect/mfa/

2. Configure SSHProxy for password less login and download the nersc private keys  in .ssh folder.
	https://docs.nersc.gov/connect/mfa/#sshproxy


3. Enable the ssh config as below to login so Pilot can login just using ```ssh localhost``` or compute nodes like ``` ssh nid7648```

cat ~/.ssh/config 
Host localhost perlmutter.nersc.gov nid* dtn*.nersc.gov
   IdentityFile ~/.ssh/nersc
   IdentitiesOnly yes
   ForwardAgent yes   		

4. copy the private key ~/.ssh/nersc to ~/.ssh/mykey, so dash can use the private key to start scheduler/worker on the compute nodes once provisioned. TODO: Not sure why nersc private key isn't working.

5. install pilot-streaming from branch(git clone & do python setup.py install) - https://github.com/radical-cybertools/pilot-streaming/tree/support-perl. Once verified we can merge these changes.

6. Execute python ps-dask.py on perlmutter login node.
