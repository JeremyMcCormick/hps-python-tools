"""
Base tasks in Luigi for batch jobs.

@author Jeremy McCormick (SLAC)
"""

import luigi
import os

from hps.batch.config import hps as hps_config
hps_config().setup()

from hps.batch.util import run_process
from hps.batch.config import hps as hps_config
from hps.lcio.event_proc import EventManager
from hps.contrib.sim_plots import SimPlotsProcessor

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
    
class SlicBaseTask(luigi.Task):
    
    detector = luigi.Parameter(default='HPS-PhysicsRun2016-Pass2')
    init_macro = luigi.Parameter(default='slic_init.mac')
    output_file = luigi.Parameter(default='slicEvents.slcio')
    nevents = luigi.IntParameter(default=10000)
    gen_macro = luigi.Parameter(default='gun.mac')
    physics_list = luigi.Parameter(default='QGSP_BERT')

    def run(self):

        config = hps_config()
         
        slic_env = config.slic_setup_script
       
        lcdd_path = config.get_lcdd_path(self.detector)
        
        config.create_fieldmap_symlink()
                
        run_script_name = self.task_id + '.sh'
        run_script = open(run_script_name, 'w')
        run_script.write('#!/bin/bash\n')
        run_script.write('. %s\n' % slic_env)
        run_script.write('slic -g %s -l %s -m %s -m %s -o %s -r %d\n' % 
                         (lcdd_path, self.physics_list, self.init_macro, 
                          self.gen_macro, self.output_file, self.nevents))
        run_script.close()
        
        os.chmod(run_script.name, 0700)
        
        cmd = './%s' % run_script.name

        try:        
            run_process(cmd)
        finally:
            os.remove(run_script.name)
            #os.unlink('fieldmap')
        
    def output(self):
        return luigi.LocalTarget(self.output_file)
    
class HpsSimBaseTask(luigi.Task):
    
    detector = luigi.Parameter(default='HPS-PhysicsRun2016-Pass2')
    #init_macro = luigi.Parameter(default='sim_init.mac')
    output_file = luigi.Parameter(default='slicEvents.slcio')
    nevents = luigi.IntParameter(default=10000)
    gen_macro = luigi.Parameter(default='sim_gun.mac')
    physics_list = luigi.Parameter(default='QGSP_BERT')    
    output_file = luigi.Parameter(default="simEvents.slcio")

    def run(self):
        
        config = hps_config()
        
        sim_env = config.sim_setup_script
        
        config.create_fieldmap_symlink()
        
        lcdd_path = config.get_lcdd_path(self.detector)

        run_macro_name = self.task_id + '.mac'
        run_macro = open(run_macro_name, 'w')
        run_macro.write('/lcdd/url %s\n' % lcdd_path)
        run_macro.write('/hps/physics/list %s\n' % self.physics_list)
        run_macro.write('/run/initialize\n')
        run_macro.write('/hps/plugins/load EventPrintPlugin\n')
        run_macro.write('/hps/plugins/EventPrintPlugin/modulus 10\n')
        run_macro.write('/random/setSeeds 1234 56789\n') # FIXME: hard-coded random init
        run_macro.write('/hps/lcio/file %s\n' % self.output_file)
        run_macro.write('/control/execute %s\n' % self.gen_macro)
        run_macro.write('/run/beamOn %d\n' % self.nevents)
        run_macro.close()
        
        run_script_name = self.task_id + '.sh'
        run_script = open(run_script_name, 'w')
        run_script.write('#!/bin/bash\n')
        run_script.write('. %s\n' % sim_env)
        run_script.write('hps-sim %s\n' % run_macro.name)
        run_script.close()
        
        os.chmod(run_script.name, 0700)
        
        cmd = './%s' % run_script.name
        
        try:
            run_process(cmd)
        finally:
            os.remove(run_script.name)
            #os.remove(run_macro.name)
        
    def output(self):
        return luigi.LocalTarget(self.output_file)
        
class OverlayBaseTask(luigi.Task):
    
    label1 = luigi.Parameter(default="slic")
    label2 = luigi.Parameter(default="hpssim")
     
    def output(self):
        return luigi.LocalTarget("simCompare.pdf")

    def run(self):
        import hps.contrib as _contrib
        compare_script = '%s/%s' % (os.path.dirname(_contrib.__file__), 'ComparePlots.py')
        cmd = "python %s simCompare %s %s %s %s" % (compare_script, self.input()[0].path, self.input()[1].path, self.label1, self.label2)
        run_process(cmd)
        
class SimAnalBaseTask(luigi.Task):
    
    plot_file = luigi.Parameter(default="plots.root")
    
    def run(self):
        processors = [SimPlotsProcessor("MySimPlotsProcessor", self.output().path)]
        files = [self.input().path]
        mgr = EventManager(processors, files)
        mgr.processEvents() 
    
    def output(self):
        return luigi.LocalTarget(self.plot_file)
            