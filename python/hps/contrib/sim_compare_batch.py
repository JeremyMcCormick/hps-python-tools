#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sun Mar  3 11:34:38 2019

@author: Jeremy McCormick (SLAC)
"""

import luigi
import os, subprocess
from hps.lcio.event_proc import EventManager
from hps.contrib.sim_plots import SimPlotsProcessor

def run_process(c):
    print "Running: " + str(c)
    print type(c)
    if isinstance(c, basestring):
        cmd = c.split()
    elif isinstance(c, tuple) or isinstance(c, list):
        cmd = c
    else:
        raise Exception("Bad command argument to run_process")
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print("OUTPUT:")
    for l in p.stdout:
        print(l.strip())
    print()
    print("ERRORS: ")
    for l in p.stderr:
        print(l.strip())
    print()
    print("returncode: " + str(p.returncode))

class SlicTask(luigi.Task):
    
    geom = luigi.Parameter(default="HPS-EngRun2015-Nominal-v3-5-3-fieldmap.lcdd")
    mac = luigi.Parameter(default="slic_gun.mac")
    output_file = luigi.Parameter(default="slicEvents.slcio")
    
    def run(self):        
        cmd = ['slic', '-g', self.geom, '-m', self.mac]
        run_process(cmd)
        
    def output(self):
        return luigi.LocalTarget(self.output_file)
    
class SimTask(luigi.Task):
    
    mac = luigi.Parameter(default="sim_gun.mac")
    output_file = luigi.Parameter(default="simEvents.slcio")
    
    def run(self):
        cmd = ['hps-sim', self.mac]
        run_process(cmd)
        
    def output(self):
        return luigi.LocalTarget(self.output_file)
    
class AnalTask(luigi.Task):
    
    plot_file = luigi.Parameter(default="plots.root")
    
    def run(self):
        processors = [SimPlotsProcessor("MySimPlotsProcessor", self.output().path)]
        files = [self.input().path]
        mgr = EventManager(processors, files)
        mgr.processEvents() 
    
    def output(self):
        return luigi.LocalTarget(self.plot_file)
    
class SlicAnalTask(AnalTask):
    
    def requires(self):
        return SlicTask()
    
class SimAnalTask(AnalTask):
    
    def requires(self):
        return SimTask()
    
class OverlayTask(luigi.Task):
    
    def requires(self):
        return (SlicAnalTask(plot_file="slicPlots.root"), SimAnalTask(plot_file="simPlots.root"))
    
    def output(self):
        return luigi.LocalTarget("simCompare.pdf")
    
    def run(self):
        
        # FIXME: use inputs instead of hard-coded file names
        cmd = "python ComparePlots.py simCompare slicPlots.root simPlots.root slic hpssim".split()
        run_process(cmd)
        
class SimCompareTask(luigi.WrapperTask):
    
    def requires(self):
        yield OverlayTask()
        
if __name__ == "__main__":
    luigi.run()
    