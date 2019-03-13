#!/usr/bin/env python2
"""
Batch job to compare MC output from SLIC and hps-sim by generating electron gun events,
creating plots, and then overlaying them in ROOT to produce a PDF.

@author: Jeremy McCormick (SLAC)
"""

import luigi

from hps.batch.config import hps as hps_config
hps_config().setup()

from hps.batch.tasks import SlicBaseTask, OverlayBaseTask, HpsSimBaseTask, CleanOutputsMixin
from hps.batch.jobs.sim_plots import SimPlotsProcessor
from hps.lcio.proc import EventManager

class SimAnalBaseTask(luigi.Task):
    
    plot_file = luigi.Parameter(default="plots.root")
    
    def run(self):
        processors = [SimPlotsProcessor("MySimPlotsProcessor", self.output().path)]
        files = [self.input().path]
        mgr = EventManager(processors, files)
        mgr.processEvents() 
    
    def output(self):
        return luigi.LocalTarget(self.plot_file)
      
class SlicAnalTask(SimAnalBaseTask):
    
    def requires(self):
        return SlicBaseTask()
    
class SimAnalTask(SimAnalBaseTask):
    
    def requires(self):
        return HpsSimBaseTask()
    
class OverlayTask(OverlayBaseTask):
 
    def requires(self):
        return (SlicAnalTask(plot_file="slicPlots.root"), 
                SimAnalTask(plot_file="simPlots.root"))

class SimCompareTask(luigi.WrapperTask, CleanOutputsMixin):
            
    def __init__(self, *args, **kwargs):
            
        super(luigi.WrapperTask, self).__init__(*args, **kwargs)
        self.clean_outputs()
                    
    def requires(self):
        yield OverlayTask()
        
if __name__ == "__main__":
    luigi.run()
    