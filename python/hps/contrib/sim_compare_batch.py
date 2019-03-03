#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sun Mar  3 11:34:38 2019

@author: jermc
"""

import luigi
import os, subprocess
from hps.lcio.event_proc import EventManager
from hps.contrib.sim_plots import SimPlotsProcessor

"""
jermc@thinksgiving:/work/slac/projects/sim_compare$ echo $PYTHONPATH
/sw/root/install/lib:/work/slac/sim/lcio/LCIO-02-12-01/install//python:/work/slac/hps-lcio-tools/python/hps/contrib/:/work/slac/hps-lcio-tools/python/
"""

class SlicTask(luigi.Task):
    
    geom = luigi.Parameter(default="HPS-EngRun2015-Nominal-v3-5-3-fieldmap.lcdd")
    mac = luigi.Parameter(default="slic_gun.mac")
    output_file = luigi.Parameter(default="slicEvents.slcio")
    
    def run(self):        
        cmd = ['slic', '-g', self.geom, '-m', self.mac]
        print "running " + str(cmd)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        for l in p.stdout:
            print l
        p.wait()
        print p.returncode
        
    def output(self):
        return luigi.LocalTarget(self.output_file)
    
class SimAnalTask(luigi.Task):
    
    plot_file = luigi.Parameter(default="simPlots.root")
    
    def run(self):
        processors = [SimPlotsProcessor("MySimPlotsProcessor", self.output().path)]
        files = [self.input().path]
        mgr = EventManager(processors, files)
        mgr.processEvents() 
    
    def output(self):
        return luigi.LocalTarget(self.plot_file)
    
    def requires(self):
        return SlicTask()

class SimCompareTask(luigi.Task):
    
    def requires(self):
        return SimAnalTask()

if __name__ == "__main__":
    luigi.run()
    