"""
Base tasks in Luigi for batch jobs.

@author Jeremy McCormick (SLAC)
"""

"""
TODO List

Tasks
    - LCIO command line tool (print, count & concat)
    - DQM data pipeline (mostly will put these tasks in hps.batch.jobs)
    - eviocopy (requires EVIO project)

Batch Submission
    - bsub
    - Auger
    
Features:
    - implement progress bar
    - support input files vs command line file lists (both?)
"""

import luigi
import os, shutil, stat

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

class StdhepFileListTask(luigi.Task):
    
    stdhep_files = luigi.ListParameter()
    
    def output(self):
        [luigi.LocalTarget(f) for f in self.stdhep_files]

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
        
        os.chmod(run_script.name, stat.S_IEXEC)
        
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
    """
    usage: java org.lcsim.job.JobControlManager [options] steeringFile.xml
    -b,--batch               Run in batch mode in which plots will not be
                              shown.
    -D,--define <arg>        Define a variable with form [name]=[value]
    -d,--detector <arg>      user supplied detector name (careful!)
    -e,--event-print <arg>   Event print interval
    -h,--help                Print help and exit
    -i,--input-file <arg>    Add an LCIO input file to process
    -n,--nevents <arg>       Set the max number of events to process
    -p,--properties <arg>    Load a properties file containing variable
                              definitions
    -r,--resource            Use a steering resource rather than a file
    -R,--run <arg>           user supplied run number (careful!)
    -s,--skip <arg>          Set the number of events to skip
    -t,--tag <arg>           conditions system tag (can be used multiple
                              times)
    -w,--rewrite <arg>       Rewrite the XML file with variables resolved
    -x,--dry-run             Perform a dry run which does not process events
    """

    steering = luigi.Parameter(default=job_config().recon_steering)

    resource = luigi.BoolParameter(default=True) # FIXME: must be false
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
       
# TODO: test me
class LcioPrintBaseTask(luigi.Task):
    lcio_cmd = luigi.Parameter(default='print')

    def run(self):
        lcio_jar = hps_config().lcio_jar
        cmd = ['java', '-jar', lcio_jar, self.lcio_cmd]
        for i in luigi.task.flatten(self.input()):
            cmd.append('-f %s' % i.path)
        run_process(cmd)

class CopyFilesBaseTask(luigi.Task):
    
    output_dir = luigi.Parameter()
    create_dir = luigi.BoolParameter(default=False)
    
    def run(self):    
        if not os.path.isdir(self.output_dir):
            if self.create_dir:
                os.makedirs(self.output_dir)
            else:
                raise Exception("The output dir does not exist: %s" % self.output_dir)
                
        if not os.path.isdir(self.output_dir):
            raise Exception("Failed to create output dir: %s" % self.output_dir)
            
        for i in luigi.task.flatten(self.input()):
            print("Copying %s to %s ..." % (i.path, self.output_dir))
            shutil.copyfile(i.path, "%s/%s" % (self.output_dir, os.path.basename(i.path)))
        

class EvioToLcioBaseTask(luigi.Task):

    """
    EvioToLcio [options] [evioFiles]
    usage:
     -b         enable headless mode in which plots will not show
     -d <arg>   detector name (required)
     -D <arg>   define a steering file variable with format -Dname=value
     -e <arg>   event printing interval
     -f <arg>   text file containing a list of EVIO files
     -h         print help and exit
     -l <arg>   path of output LCIO file
     -m <arg>   set the max event buffer size
     -M         use memory mapping instead of sequential reading
     -n <arg>   maximum number of events to process in the job
     -r         interpret steering from -x argument as a resource instead of a
                file
     -R <arg>   fixed run number which will override run numbers of input
                files
     -s <arg>   skip a number of events in each EVIO input file before
                starting
     -t <arg>   specify a conditions tag to use
     -v         print EVIO XML for each event
     -x <arg>   LCSim steeering file for processing the LCIO events
    """
        
    detector = luigi.Parameter()
    run_number = luigi.IntParameter(default=-1)
    evio_files = luigi.ListParameter()
    event_print_interval = luigi.IntParameter(default=-1)
    output_file = luigi.Parameter() # no extension
    nevents = luigi.IntParameter(default=-1)
    steering = luigi.Parameter(default=None)
    resource = luigi.BoolParameter(default=False)  
    headless = luigi.BoolParameter(default=True)
    write_raw_output = luigi.BoolParameter(default=False)
    raw_output_file = luigi.Parameter(default='raw.slcio') # usually not used
    output_ext = luigi.Parameter(default='.slcio') # override for .root or .aida
    
    def run(self):
        config = hps_config()
        bin_jar = config.hps_java_bin_jar
        
        cmd = ['java', '-cp', bin_jar, 'org.hps.evio.EvioToLcio']

        cmd.append('-d %s' % self.detector)
        if self.run_number != -1:
            cmd.append('-R %d' % self.run_number)
        if self.event_print_interval != -1:
            cmd.append('-e %d' % self.event_print_interval)
        cmd.append('-DoutputFile=%s' % self.output_file)
        if self.nevents != -1:
            cmd.append('-n %d' % self.nevents)
        if self.resource:
            cmd.append('-r')    
        if self.steering is not None:
            cmd.append('-x %s' % self.steering)
        if self.headless:
            cmd.append('-b')
        if self.write_raw_output:
            cmd.append('-l %s' % self.lcio_output_file)        
        for i in self.evio_files:
            cmd.append(i)
                    
        run_process(cmd, use_shell=True)
        
    def output(self):
        # This doesn't include the raw data output which usually we don't care about.
        return luigi.LocalTarget("%s%s" % (self.output_file, self.output_ext))