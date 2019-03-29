"""
Compare recon output between slic and hps-sim.
"""

# sim <- filter <- readout <- recon <- anal

import luigi

from hps.batch.config import hps as hps_config
hps_config().setup()

from hps.batch.tasks import FilterMCBunchesBaseTask, SlicBaseTask, JobManagerBaseTask
from hps.batch.jobs.sim_compare import SimAnalBaseTask

class SlicFilterTask(FilterMCBunchesBaseTask):
    
    def requires(self):
        return SlicBaseTask(output_file="slicEvents.slcio", nevents=100)

    def outputs(self):
        return luigi.LocalTarget(self.output_file)

class SlicReadoutTask(JobManagerBaseTask):
        
    def requires(self):
        return SlicFilterTask(output_file="slicFilteredEvents.slcio", nevents=25000)
        
class SlicReconTask(JobManagerBaseTask):
        
    def requires(self):

        return SlicReadoutTask(steering="/org/hps/steering/readout/PhysicsRun2016TrigSingles1.lcsim",
                               output_file="slicReadoutEvents.slcio")
        #return SlicReadoutTask(steering="/org/hps/steering/readout/PhysicsRun2016TrigPairs1.lcsim",
        #                       output_file="slicReadoutEvents.slcio")
        
class SlicReconAnalTask(SimAnalBaseTask):
                
    def requires(self):
        return SlicReconTask(steering="/org/hps/steering/recon/PhysicsRun2016FullReconMC.lcsim",
                             output_file="slicReconEvents.slcio")
    
class SimReconCompareTask(luigi.WrapperTask):
        
    def requires(self):
        yield SlicReconAnalTask(plot_file="slicPlots.root")
        
class DerpTask(luigi.Task):

    derp = luigi.Parameter(default=None)
    
    def run(self):
        print(self.derp)
        d = self.derp
        print(d)
        
    def output(self):
        return luigi.LocalTarget('derp.derp')
