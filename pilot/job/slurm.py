#!/usr/bin/env python

import textwrap
import re
import os
import pdb
import logging
import subprocess
import math
from urllib.parse import urlparse
import tempfile

logging.basicConfig(datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='pilot-streaming')

# logger.basicConfig(datefmt='%m/%d/%Y %I:%M:%S %p',
#           format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.DEBUG)


#import saga


class Service(object):
    """ Plugin for SlURM    """

    def __init__(self, resource_url):
        """Constructor"""
        self.resource_url = resource_url

    def create_job(self, job_description):
        j = Job(job_description, self.resource_url)
        return j

    def __del__(self):
        pass


class Job(object):
    """Constructor"""

    def __init__(self, job_description, resource_url):
        self.job_description = job_description
        self.command = self.job_description["executable"]
        if "arguments" in self.job_description:
            self.command = (("%s %s") % (self.job_description["executable"],
                                                  self.job_description["arguments"]))
        self.job_id = ""
        self.resource_url = resource_url
        #self.resource_url.scheme = "ssh"
        logger.debug("Pilot-Streaming SLURM: Parsing job description")
        self.pilot_compute_description = {}
        if 'queue' in job_description: self.pilot_compute_description['queue'] = job_description['queue']
        if 'project' in job_description: self.pilot_compute_description['project'] = job_description['project']
        self.pilot_compute_description['working_directory'] = os.getcwd()
        if 'working_directory' in job_description: self.pilot_compute_description['working_directory'] = job_description['working_directory']
        if 'walltime' in job_description: self.pilot_compute_description['walltime'] = job_description['walltime']

        self.pilot_compute_description['number_cores']=48
        if 'number_cores' in job_description: self.pilot_compute_description['number_cores'] = job_description['number_cores']
            
        self.pilot_compute_description['number_of_nodes'] = 1
        if 'number_of_nodes' in job_description: self.pilot_compute_description['number_of_nodes'] = job_description['number_of_nodes']

        self.working_directory = self.pilot_compute_description["working_directory"]
        ### convert walltime in minutes to SLURM representation of time ###
        walltime_slurm = "01:00:00"
        if "walltime" in self.pilot_compute_description:
            hrs = math.floor(int(self.pilot_compute_description["walltime"]) / 60)
            minu = int(self.pilot_compute_description["walltime"]) % 60
            walltime_slurm = "" + str(hrs) + ":" + str(minu) + ":00"
        self.pilot_compute_description["walltime_slurm"]=walltime_slurm

        logger.debug("Pilot-Streaming SLURM: generate bootstrap script")
        # sbatch_file.write("python -c XX" + textwrap.dedent(\"\"%s\"\") + "XX")

        self.bootstrap_script = textwrap.dedent("""import sys
import os
import urllib
import sys
import time
import textwrap

sbatch_file_name="pilotstreaming_slurm_ssh"

sbatch_file = open(sbatch_file_name, "w")
sbatch_file.write("#!/bin/bash")
sbatch_file.write("\\n")
sbatch_file.write("#SBATCH -n %s")
sbatch_file.write("\\n")
sbatch_file.write("#SBATCH -N %s")
sbatch_file.write("\\n")
sbatch_file.write("#SBATCH -J pilot-streaming-slurm")
sbatch_file.write("\\n")
sbatch_file.write("#SBATCH -t %s")
sbatch_file.write("\\n")
sbatch_file.write("#SBATCH -A %s")
sbatch_file.write("\\n")
sbatch_file.write("#SBATCH -o %s/stdout-pilotstreaming-spark.txt")
sbatch_file.write("\\n")
sbatch_file.write("#SBATCH -e %s/stderr-pilotstreaming-spark.txt")
sbatch_file.write("\\n")
sbatch_file.write("#SBATCH -p %s")
sbatch_file.write("\\n")

sbatch_file.write("cd %s")
sbatch_file.write("\\n")
sbatch_file.write("%s")
sbatch_file.close()
#os.system( "sbatch  " + sbatch_file_name)
""") % (str(self.pilot_compute_description["number_cores"]), str(self.pilot_compute_description["number_of_nodes"]),
        str(walltime_slurm),
        str(self.pilot_compute_description["project"]), self.pilot_compute_description["working_directory"],
        self.pilot_compute_description["working_directory"], self.pilot_compute_description["queue"],
        self.pilot_compute_description["working_directory"], self.command)
        ### escaping characters
        self.bootstrap_script = self.bootstrap_script.replace("\"", "\\\"")
        self.bootstrap_script = self.bootstrap_script.replace("\\\\", "\\\\\\\\\\")
        self.bootstrap_script = self.bootstrap_script.replace("XX", "\\\\\\\"")
        self.bootstrap_script = "\"" + self.bootstrap_script + "\""
        logger.debug(self.bootstrap_script)

    def run(self):
        o = urlparse("slurm+ssh://login1.wrangler.tacc.utexas.edu")
        target_host = o.netloc
        start_command=("ssh %s "%target_host)
        tmpf_name = ""
        logger.debug("Submit pilot job to: " + str(self.resource_url))
        try:
            fd, tmpf_name = tempfile.mkstemp()
            print(tmpf_name)
            with os.fdopen(fd, 'w') as tmp:
                tmp.write("#!/bin/bash")
                tmp.write("\n")
                tmp.write("#SBATCH -n %s"%str(self.pilot_compute_description["number_cores"]))
                tmp.write("\n")
                tmp.write("#SBATCH -N %s"%str(self.pilot_compute_description["number_of_nodes"]))
                tmp.write("\n")
                tmp.write("#SBATCH -J pilot-streaming-slurm")
                tmp.write("\n")
                tmp.write("#SBATCH -t %s"%str(self.pilot_compute_description["walltime_slurm"]))
                tmp.write("\n")
                tmp.write("#SBATCH -A %s"%str(self.pilot_compute_description["project"]))
                tmp.write("\n")
                tmp.write("#SBATCH -o %s/stdout-pilotstreaming-spark.txt"%self.pilot_compute_description["working_directory"])
                tmp.write("\n")
                tmp.write("#SBATCH -e %s/stderr-pilotstreaming-spark.txt"%self.pilot_compute_description["working_directory"])
                tmp.write("\n")
                tmp.write("#SBATCH -p %s"%self.pilot_compute_description["queue"])
                tmp.write("\n")
                tmp.write("cd %s"%self.pilot_compute_description["working_directory"])
                tmp.write("\n")
                tmp.write("%s"%self.command)
                tmp.flush()
                start_command = ("scp %s %s:~/"%(tmpf_name, target_host))
                status = subprocess.call(start_command, shell=True)
        finally:
            pass
            #os.remove(tmpf)

        start_command = ("ssh %s "%target_host)
        start_command = start_command + ("sbatch  %s"%os.path.basename(tmpf_name))
        print(("Submission of Job Command: %s"%start_command))
        status = subprocess.call(start_command, shell=True)
        logger.debug("Pilot-Streaming SLURM: SSH run job finished")
        #saga_surl = saga.Url(self.resource_url)
        #sftp_url = "sftp://"
        #if saga_surl.username != None and saga_surl.username != "":
        #    sftp_url = sftp_url + str(saga_surl.username) + "@"
        #sftp_url = sftp_url + saga_surl.host + "/"
        #outfile = sftp_url + self.working_directory + '/saga_job_submission.out'
        #logger.debug("BigJob/SLURM: get outfile: " + outfile)
        #out = saga.filesystem.File(outfile)
        #out.copy("sftp://localhost/" + os.getcwd() + "/tmpout")
        #errfile = sftp_url + self.working_directory + '/saga_job_submission.err'
        #err = saga.filesystem.File(errfile)
        #err.copy("sftp://localhost/" + os.getcwd() + "/tmperr")
#
        #tempfile = open(os.getcwd() + "/tmpout")
        #outstr = tempfile.read().rstrip()
        #tempfile.close()
        #os.remove(os.getcwd() + "/tmpout")
#
        #tempfile = open(os.getcwd() + "/tmperr")
        #errstr = tempfile.read().rstrip()
        #tempfile.close()
        #os.remove(os.getcwd() + "/tmperr")
#
        #logger.debug("Output - \n" + str(outstr))
        #if ((outstr).split("\n")[-1]).split()[0] == "Submitted":
        #    self.job_id = ((outstr).split("\n")[-1]).split()[3]
        #    logger.debug("SLURM JobID: " + str(self.job_id))
        if self.job_id == None or self.job_id == "":
            raise Exception("Pilot-Streaming Submission via slurm+ssh:// failed")

    def get_state(self):
        start_command=("%s %s %s"%("squeue", "-j", self.job_id ))
        output = subprocess.check_output(start_command, shell=True)

        #jd = saga.job.Description()#
        #jd.executable = "squeue"
        #jd.arguments = ["-j", self.job_id]
        #jd.output = "jobstate.out"
        #jd.working_directory = self.working_directory
        ## connect to the local job service
        #js = saga.job.service(self.resource_url);
        ## submit the job
        #job = js.create_job(jd)
        #job.run()
        #job.wait()
        # print the job's output

        #outfile = 'sftp://' + saga.Url(self.resource_url).host + self.working_directory + '/jobstate.out'
        #out = saga.filesystem.File(outfile)
        #out.move("sftp://localhost/" + os.getcwd() + "/tmpstate")

        #tempfile = open(os.getcwd() + "/tmpstate")
        #output = tempfile.read().rstrip()
        #tempfile.close()
        #os.remove(os.getcwd() + "/tmpstate")

        state = output.split("\n")[-1].split()[4]

        if state.upper() == "R":
            state = "Running"
        elif state.upper() == "CD" or state.upper() == "CF" or state.upper() == "CG":
            state = "Done"
        elif state.upper() == "PD":
            state = "Queue"
        else:
            state = "Unknown"
        return state

    def cancel(self):
        logger.debug("Cancel SLURM job")
        jd = {}
        jd["executable"] = "scancel"
        jd.arguments = [self.job_id]
        # connect to the local job service
        js = Service(self.resource_url);
        # submit the job
        job = js.create_job(jd)
        job.run()
        # wait for the job to complete
        job.wait()


if __name__ == "__main__":
    slurm_service = Service("slurm+ssh://login1.wrangler.tacc.utexas.edu")

    jd ={
        "executable":"/bin/date",
        "resource":"slurm://localhost",
        "working_directory": os.path.join('/work/01131/tg804093/wrangler/', "work"),
        "number_cores": 48,
        "number_of_nodes": 1,
        "project": "TG-MCB090174",
        "queue": "normal",
        "walltime": 59,
    }
    j = slurm_service.create_job(jd)
    j.run()
    print(j.get_state())
