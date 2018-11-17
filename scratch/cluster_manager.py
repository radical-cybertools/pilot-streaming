from pygtail import Pygtail
import subprocess

 # Header: BatchTime, SubmissionTime, SchedulingDelay, ProcessingDelay, TotalDelay, NumberRecords, Window, Scenario


class ClusterManager():

	def __init__(self, monitor_url=None, classifier='naive'):

        self.classifier = classifier
		if monitor_url:
            self.monitor_url = monitor_url
        else:
           try:
                self.monitor_url = os.environ.get('MONITOR_URL')
            except Exception as e:
                print 'monitor_url is not set'
                raise e
        return

    def get_metrics(self):

        for line in tailer.follow(open(self.monitor_url)):  # file should exist
            line_split = line.split(',')
            self.schedulingDelay = float(line_split[2])
            self.processingDelay = float(line_split[3])
            self.totalDelay = float(line_split[4])
            self.numberRecords = float(line_split[5])
            self.streaming_window = float(line_split[6])
            self._predict()

        return


    def _predict(self):
        # this is a naive classifier
        aclassifier = Classifiers(self.SchedulingDelay, self.processingDelay,
            self.streaming_window, self.numberRecords)

        if self.classifier =='naive':
            return aclassifier.naive_classifier()
        else:
            pass


     def run(self):  
        cmd = self.get_metrics() 
        check_output(cmd, shell=True)
        
        
    def run_in_background(self):  
        cmd = self.get_metrics()
        self.manager_process = subprocess.Popen(cmd, shell=True)

    def cancel(self):
        self.manager_process.kill()




class Classifiers():

    def __init__(self,schedulingDelay,processingDelay,streaming_window,
                numberRecords):
        self.schedulingDelay = schedulingDelay
        self.processingDelay = processingDelay
        self.streaming_window = streaming_window
        self.numberRecords = numberRecords
        ## constants
        self.add_node ='add_node'
        self.remove_node = 'remove_node'
        self.stable_streaming = 'stable_streaming'



    def naive_classifier(self):  #TODO: make threasholds variables?
        if self.schedulingDelay > 5 and self.processingDelay > self.streaming_window:
            return self.add_node
        elif self.schedulingDelay < 5 and self.processingDelay <= self.streaming_window-15:
            return self.remove_node
        else
            return self.stable_streaming


    def logistic_regression_classifier(self):
        pass

    def KNearestNeighbor_classifier(self):


#if __name__=='__main__':

#    manager = ClusterManager(monitor_url='aurl',classifier='naive')
#    manager.run()
#    manager.cancel()


 # dowhile = True
 #        while blocking or dowhile:  # do-while
 #            dowhile = False
 #            for line in Pygtail(self.monitor_url)
 #                line_split = line.split(',')  

 #                self.schedulingDelay = float(line_split[2])
 #                self.processingDelay = float(line_split[3])
 #                self.totalDelay = float(line_split[4])
 #                self.numberRecords = float(line_split[5])
 #                self.streaming_window = float(line_split[6])
 #                self._predict()

        










