class Service(object):
    """ Base class to manage Service """
    def __init__(self, resource_url):
        self.resource_url = resource_url

    def create_job(self, job_description):
        raise NotImplementedError("start() method must be implemented in the subclass.")