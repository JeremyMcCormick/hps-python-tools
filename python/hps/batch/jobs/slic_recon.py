import luigi

from hps.batch.config import hps as hps_config
hps_config().setup()

from hps.batch.tasks import FilterMCBunchesBaseTask, JobManagerBaseTask, SlicStdhepBaseTask
    
# run slic with stdhep file    
class SlicTask(SlicStdhepBaseTask):
 
    pass

# run event filtering to space events
class SlicFilterTask(FilterMCBunchesBaseTask):
    
    def requires(self):
        return SlicTask()

    def outputs(self):
        return luigi.LocalTarget(self.output_file)

# run the readout
# steering and other params to be defined at CL
class SlicReadoutTask(JobManagerBaseTask):
        
    def requires(self):
        return SlicFilterTask()
        
# run the recon
# steering and other params to be defined at CL
class SlicReconTask(JobManagerBaseTask):
        
    def requires(self):
        return SlicReadoutTask()
    
#    steering="/org/hps/steering/readout/PhysicsRun2016TrigSingles1.lcsim"
