# -*- coding: utf-8 -*-
"""
Luigi tasks
"""

import luigi
import os

from hps.batch.util import run_process

from hps.lcio.event_proc import EventManager
from hps.contrib.sim_plots import SimPlotsProcessor

class SlicBaseTask(luigi.Task):
    
    geom = luigi.Parameter(default="HPS-EngRun2015-Nominal-v3-5-3-fieldmap.lcdd")
    mac = luigi.Parameter(default="slic_gun.mac")
    output_file = luigi.Parameter(default="slicEvents.slcio")
    
    def run(self):        
        # TODO: replace with shell command including env setup script
        cmd = ['slic', '-g', self.geom, '-m', self.mac]
        run_process(cmd)
        
    def output(self):
        return luigi.LocalTarget(self.output_file)
    
class SimBaseTask(luigi.Task):
    
    mac = luigi.Parameter(default="sim_gun.mac")
    output_file = luigi.Parameter(default="simEvents.slcio")
    
    def run(self):
        # TODO: replace with shell command including env setup script
        cmd = ['hps-sim', self.mac]
        run_process(cmd)
        
    def output(self):
        return luigi.LocalTarget(self.output_file)
        
class OverlayBaseTask(luigi.Task):
    
    label1 = luigi.Parameter(default="slic")
    label2 = luigi.Parameter(default="hpssim")
     
    def output(self):
        return luigi.LocalTarget("simCompare.pdf")

    # TODO: ComparePlots.py should come from contrib directory of this project or python code should be called directly.
    def run(self):
        cmd = "python ComparePlots.py simCompare %s %s %s %s" % (self.input()[0].path, self.input()[1].path, self.label1, self.label2)
        run_process(cmd)
        
class AnalBaseTask(luigi.Task):
    
    plot_file = luigi.Parameter(default="plots.root")
    
    def run(self):
        processors = [SimPlotsProcessor("MySimPlotsProcessor", self.output().path)]
        files = [self.input().path]
        mgr = EventManager(processors, files)
        mgr.processEvents() 
    
    def output(self):
        return luigi.LocalTarget(self.plot_file)
    
class CleanOutputsMixin:
    
    force = luigi.BoolParameter(default=False)
    
    def clean_outputs(self):
        if self.force is True:
            done = False
            tasks = [self]
            while not done:
                outputs = luigi.task.flatten(tasks[0].output())
                [os.remove(out.path) for out in outputs if out.exists()]
                tasks += luigi.task.flatten(tasks[0].requires())
                tasks.pop(0)
                if len(tasks) == 0:
                    done = True
        