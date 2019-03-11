#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Compare MC output from SLIC and hps-sim by generating electron gun events,
generating plots and overlaying them in ROOT.

@author: Jeremy McCormick (SLAC)
"""

import luigi
from hps.batch.tasks import SlicBaseTask, OverlayBaseTask, AnalBaseTask, SimBaseTask, CleanOutputsMixin
       
class SlicAnalTask(AnalBaseTask):
    
    def requires(self):
        return SlicBaseTask()
    
class SimAnalTask(AnalBaseTask):
    
    def requires(self):
        return SimBaseTask()
    
class OverlayTask(OverlayBaseTask):
 
    def requires(self):
        return (SlicAnalTask(plot_file="slicPlots.root"), SimAnalTask(plot_file="simPlots.root"))
        
class SimCompareTask(luigi.WrapperTask, CleanOutputsMixin):
        
    def __init__(self, *args, **kwargs):
            
        super(luigi.WrapperTask, self).__init__(*args, **kwargs)
        self.clean_outputs()
                    
    def requires(self):
        yield OverlayTask()
        
if __name__ == "__main__":
    luigi.run()
    