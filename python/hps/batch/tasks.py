"""
Base tasks in Luigi for batch jobs.

@author Jeremy McCormick (SLAC)
"""

"""
TODO:
    - LCIO command line tool
    - EvioToLcio
    - implement progress bar
"""

import luigi
import os

from hps.batch.util import run_process
from hps.batch.config import hps as hps_config
from hps.batch.config import job as job_config

"""
class ListTestTask(luigi.Task):
    
    files = luigi.ListParameter()
    
    def run(self):
        print "ListTestTask.run"
        for f in luigi.task.flatten(input()):
            print f
        
    def input(self):
        [luigi.LocalTarget(f) for f in self.files]
"""
        
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
                    
class FileListTask(luigi.Task):
    
    files = luigi.ListParameter()
    
    def output(self):
        [luigi.LocalTarget(f) for f in self.files]
   
"""
class StdhepFileListTask(luigi.Task):
    
    stdhep_files = luigi.ListParameter()
    
    def output(self):
        [luigi.LocalTarget(f) for f in self.stdhep_files]
"""   
   
# TODO: 
# - set run number
# - implement with sub-tasks so they can be run in parallel when using multiple input stdhep files and then combine files using LCIO tool
# - refactor so there's no code duplication with SlicBaseTask
# - possible to implement input with a parameter? (e.g. if stdhep generator run first)
class SlicStdhepBaseTask(luigi.Task):
        
    detector = luigi.Parameter(default=job_config().detector)
    nevents = luigi.IntParameter(default=job_config().nevents) # num events to run from each stdhep file
    physics_list = luigi.Parameter(default=job_config().physics_list)
    
    output_file = luigi.Parameter(default='slicEvents.slcio')

    stdhep_files = luigi.ListParameter()
    
    def run(self):
        
        if not os.access(os.getcwd(), os.W_OK):
            raise Exception("Current dir is not writable: " + os.getcwd())

        config = hps_config()

        slic_env = config.slic_setup_script
       
        lcdd_path = config.get_lcdd_path(self.detector)
        
        config.create_fieldmap_symlink()
        
        #input_files = luigi.task.flatten(self.input())
        #if len(input_files) == 0:
        #    raise Exception("No stdhep input files")
                
        init_macro = open('slic_init.mac', 'w')
        init_macro.write('/lcio/fileExists append')
        init_macro.close()
        
        run_script_name = self.task_id + '.sh'
        run_script = open(run_script_name, 'w')
        run_script.write('#!/bin/bash\n')
        run_script.write('. %s\n' % slic_env)
        for stdhep_file in self.stdhep_files:
            run_script.write('slic -g %s -l %s -m %s -i %s -o %s -r %d\n' % 
                             (lcdd_path, self.physics_list, init_macro.name, stdhep_file, self.output_file, self.nevents))
        run_script.close()
        
        os.chmod(run_script.name, 0700)
        
        cmd = './%s' % run_script.name

        try:        
            run_process(cmd)
        finally:
            os.remove(run_script.name)
            os.remove(init_macro.name)
            config.remove_fieldmap_symlink()
        
    def output(self):
        return luigi.LocalTarget(self.output_file)
    
    #def requires(self):
    #    return StdhepFileListTask()
    
class SlicBaseTask(luigi.Task):
        
    detector = luigi.Parameter(default=job_config().detector)
    nevents = luigi.IntParameter(default=job_config().nevents)
    physics_list = luigi.Parameter(default=job_config().physics_list)
    
    init_macro = luigi.Parameter(default='slic_init.mac')
    output_file = luigi.Parameter(default='slicEvents.slcio')
    gen_macro = luigi.Parameter(default='slic_gun.mac')

    def run(self):
        
        if not os.access(os.getcwd(), os.W_OK):
            raise Exception("Current dir is not writable: " + os.getcwd())

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
        
    def output(self):
        return luigi.LocalTarget(self.output_file)
    
class HpsSimBaseTask(luigi.Task):
    
    detector = luigi.Parameter(default=job_config().detector)
    nevents = luigi.IntParameter(default=job_config().nevents)
    physics_list = luigi.Parameter(default=job_config().physics_list)
    
    output_file = luigi.Parameter(default='simEvents.slcio')
    gen_macro = luigi.Parameter(default='sim_gun.mac')
    output_file = luigi.Parameter(default="simEvents.slcio")

    def run(self):
        
        if not os.access(os.getcwd(), os.W_OK):
            raise Exception("Current dir is not writable: " + os.getcwd())
        
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
            os.remove(run_macro.name)
        
    def output(self):
        return luigi.LocalTarget(self.output_file)
    
class FilterMCBunchesBaseTask(luigi.Task):
        
    ecal_hit_ecut = luigi.FloatParameter(default=0.0) # set to 0.05 for 2015 and 0.1 for 2016
    spacing = luigi.IntParameter(default=job_config().event_spacing)
    enable_ecal_energy_filter = luigi.BoolParameter(default=False)
    nevents = luigi.IntParameter(default=job_config().nevents * job_config().event_spacing)
    output_file = luigi.Parameter(default="filteredEvents.slcio")
    
    def run(self):
        config = hps_config()
        bin_jar = config.hps_java_bin_jar
                        
        cmd = ['java', '-cp', bin_jar, 'org.hps.util.FilterMCBunches',
               '-e', str(self.spacing), '-E', str(self.ecal_hit_ecut),
               '-w', str(self.nevents)]
        if self.enable_ecal_energy_filter:
            cmd.append('-d')
        for i in luigi.task.flatten(self.input()):
            cmd.append(i.path)
        for o in luigi.task.flatten(self.output()):
            cmd.append(o.path)
        #print("Running FilterMCBunches: " + " ".join(cmd))
        run_process(cmd)
            
    def output(self):
        return luigi.LocalTarget(self.output_file)
        
# TODO:
# - option to install and use the local conditions db (req some updates to config as well)
# - hps-java log config prop
class JobManagerBaseTask(luigi.Task):

    steering = luigi.Parameter(default=job_config().recon_steering)

    resource = luigi.BoolParameter(default=True)
    output_file = luigi.Parameter()
    
    run_number = luigi.IntParameter(default=None)
    detector = luigi.Parameter(default=None)
    
    nevents = luigi.IntParameter(default=None)
    
    """
    To define vars at command line:
    $ luigi --module my_tasks MyTask --tags '{"role": "web", "env": "staging"}'
    """
    define = luigi.DictParameter(default=None)
    
    batch = luigi.DictParameter(default=True)
    
    def run(self):
        config = hps_config()
        bin_jar = config.hps_java_bin_jar
        
        cmd = ['java', '-jar', bin_jar]
        if self.resource:
            cmd.append('-r')
        cmd.extend(["-i%s" % i.path for i in luigi.task.flatten(self.input())])
        
        outputs = luigi.task.flatten(self.output())
        if len(outputs) > 1:
            raise Exception("Too many outputs for this task (only one output is accepted).")
        if self.batch:
            cmd.append('-b')
        if self.detector is not None:
            cmd.append('-d %s' % self.detector)
        if self.run_number is not None:
            cmd.append('-R %d' % self.run_number)
        if self.nevents is not None:
            cmd.append('-n %d' % self.nevents)
        if self.define is not None:
            for key, value in self.define.iteritems():
                cmd.append('-D%s=%s' % (key, value))
        cmd.append("-DoutputFile=%s" % os.path.splitext(outputs[0].path)[0])
        cmd.append(self.steering)
        
        #print("Running JobManager with cmd: %s" % " ".join(cmd))
        
        # FIXME: For some reason passing the list results in a weird error with the run number :(
        run_process(cmd, use_shell=True)
        
    def output(self):
        return luigi.LocalTarget(self.output_file)
    
class OverlayBaseTask(luigi.Task):
    
    label1 = luigi.Parameter(default="slic")
    label2 = luigi.Parameter(default="hpssim")
    output_file = luigi.Parameter(default="simCompare.pdf")
     
    def output(self):
        return luigi.LocalTarget(self.output_file)

    def run(self):
        import hps.util as _util
        compare_script = '%s/%s' % (os.path.dirname(_util.__file__), 'ComparePlots.py')
        cmd = "python %s simCompare %s %s %s %s" % (compare_script, self.input()[0].path, self.input()[1].path, self.label1, self.label2)
        run_process(cmd)
        
#class LcioToolBaseTask(luigi.Task):
#    cmd = luigi.Parameter(default='print')
#    def run(self):    
#        pass
        
class LcioPrintBaseTask(luigi.Task):
    lcio_cmd = luigi.Parameter(default='print')

    def run(self):
        lcio_jar = hps_config().lcio_jar
        cmd = ['java', '-jar', lcio_jar, self.lcio_cmd]
        for i in luigi.task.flatten(self.input()):
            cmd.append('-f %s' % i.path)
        run_process(cmd)
       