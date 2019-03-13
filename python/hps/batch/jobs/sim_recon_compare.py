"""
Compare recon output between slic and hps-sim.
"""

# sim <- filter <- readout <- recon <- anal

import luigi

from hps.batch.config import hps as hps_config
hps_config().setup()

#from hps.batch.tasks import SlicBaseTask, HpsSimBaseTask, OverlayBaseTask, CleanOutputsMixin
#from hps.batch.tasks import FilterMCBunchesBaseTask

from hps.batch.tasks import FilterMCBunchesBaseTask, SlicBaseTask

class SlicFilterTask(FilterMCBunchesBaseTask):
    
    def requires(self):
        return SlicBaseTask()

    def outputs(self):
        return luigi.LocalTarget(self.output_file)

# FIXME: dummy task
class SlicReadoutTask(luigi.Task):
    
    def run(self):
        pass
    
    def requires(self):
        return SlicFilterTask(output_file="slicFilteredEvents.slcio")
    
    def output(self):
        return luigi.LocalTarget("slicReadoutEvents.slcio")

"""
class SlicAnal(luigi.Task):
    
    output_file = luigi.Parameter(default="slicPlots.root")
    
    def run(self):
        pass
    
    def output(self):
        return luigi.LocalTarget(self.output_file)
    
    #def requires(self):
"""