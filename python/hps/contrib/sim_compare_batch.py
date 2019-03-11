#!/usr/bin/env python2
"""
Batch job to compare MC output from SLIC and hps-sim by generating electron gun events,
creating plots, and then overlaying them in ROOT to produce a PDF.

@author: Jeremy McCormick (SLAC)
"""

import luigi

from hps.batch.tasks import SlicBaseTask, OverlayBaseTask, SimAnalBaseTask, HpsSimBaseTask, CleanOutputsMixin
       
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
    