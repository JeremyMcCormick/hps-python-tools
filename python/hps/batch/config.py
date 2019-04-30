"""
Luigi configuration for HPS batch jobs.

@author: Jeremy McCormick (SLAC)
"""

import luigi
import sys, os

# TODO:
# - hps-java might be better handled by having a version like '4.4-SNAPSHOT' and then getting from local repo or downloading as necessary
# - download jars from Nexus

class hps(luigi.Config):
   
    # FIXME: Do these default rel paths even work with the hps tasks? 
    slic_setup_script = luigi.Parameter(default='slic-env.sh')
    sim_setup_script = luigi.Parameter(default='hps-sim-env.sh')
    lcio_dir = luigi.Parameter(default='lcio')
    hps_java_dir = luigi.Parameter(default='hps-java')
    hps_fieldmaps_dir = luigi.Parameter(default='fieldmap')
    hps_java_bin_jar = luigi.Parameter(default='hps-java-bin.jar')
    lcio_jar = luigi.Parameter(default='lcio-bin.jar')
   
    fieldmap_symlink_name = 'fieldmap'
    
    def setup(self):
        
        # TODO: LCIO setup in separate method (often don't need it).
        if not os.path.exists(self.lcio_dir):
            raise Exception("LCIO dir does not exist: " + self.lcio_dir)
        lcio_python_dir = self.lcio_dir + '/python'
        try:
            os.environ.index(self.lcio_dir)
        except:
            os.environ['LCIO'] = self.lcio_dir
        try: 
            sys.path.index(lcio_python_dir)
        except:
            sys.path.append(lcio_python_dir)
            
        if not os.path.exists(self.slic_setup_script):
            raise Exception("SLIC setup script does not exist: " + self.slic_setup_script)
        if not os.path.exists(self.sim_setup_script):
            raise Exception("hps-sim setup script does not exist: " + self.sim_setup_script)
        if not os.path.exists(self.hps_java_dir):
            raise Exception("hps-java dir does not exist: " + self.hps_java_dir)
        if not os.path.exists(self.hps_fieldmaps_dir):
            raise Exception("Field maps directory does not exist: " + self.hps_fieldmaps_dir)
            
    def get_lcdd_path(self, detector_name):
        #print(detector_name)
        #print(self.hps_java_dir)
        detector_dir = self.hps_java_dir + '/detector-data/detectors'
        detector_path = detector_dir + '/' + detector_name + '/' + detector_name + '.lcdd'
        if not os.path.exists(detector_path):
            raise Exception("Detector does not exist (bad name?): " + detector_name)
        return detector_path

    def create_fieldmap_symlink(self):

        if os.path.exists(hps.fieldmap_symlink_name):
            print('Using existing fieldmap symlink.')
        else:
            os.symlink(self.hps_fieldmaps_dir, hps.fieldmap_symlink_name)
            print('Created fieldmap symlink to dir: ' + self.hps_fieldmaps_dir)
    
    def remove_fieldmap_symlink(self):
        if os.path.exists(hps.fieldmap_symlink_name):
            os.remove(hps.fieldmap_symlink_name)

class job(luigi.Config):

    nevents = luigi.IntParameter(default=10000)
    detector = luigi.Parameter(default='HPS-PhysicsRun2016-Pass2')
    physics_list = luigi.Parameter(default='QGSP_BERT')
    recon_steering = luigi.Parameter(default='/org/hps/steering/recon/PhysicsRun2016FullReconMC.lcsim')
    event_spacing = luigi.IntParameter(default=250)
    
class dqm(luigi.Config):
    
    user = luigi.Parameter(default='dqm-user')
    passwd = luigi.Parameter(default='12345')
    db = luigi.Parameter(default='dqm')    
    url = 'hpsdb.jlab.org'
    #url = luigi.Parameter(default='localhost')

    # FIXME: hard-coded paths for JLab
    conda_dir = luigi.Parameter(default='/group/hps/hps_soft/anaconda')
    root_dir = luigi.Parameter(default='/apps/root/6.12.06')
    hpspythontools_dir = luigi.Parameter(default='/group/hps/hps_soft/hps-python-tools')
