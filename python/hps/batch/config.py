"""
Luigi configuration for HPS batch jobs.

@author: Jeremy McCormick (SLAC)
"""

import luigi
import sys, os

class hps(luigi.Config):
   
    # FIXME: Do these default rel paths even work with the hps tasks? 
    slic_setup_script = luigi.Parameter(default='slic-env.sh')
    sim_setup_script = luigi.Parameter(default='hps-sim-env.sh')
    lcio_dir = luigi.Parameter(default='lcio')
    hps_java_dir = luigi.Parameter(default='hps-java')
    hps_fieldmaps_dir = luigi.Parameter(default='fieldmap')
    hps_java_bin_jar = luigi.Parameter(default='hps-java-bin.jar')
    
    def setup(self):
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
        detector_dir = self.hps_java_dir + '/detector-data/detectors'
        detector_path = detector_dir + '/' + detector_name + '/' + detector_name + '.lcdd'
        if not os.path.exists(detector_path):
            raise Exception("Detector does not exist (bad name?): " + detector_name)
        return detector_path
    
    def create_fieldmap_symlink(self):
        symlink_name = 'fieldmap'
        if os.path.exists(symlink_name):
            print('Using existing fieldmap symlink.')
        else:
            os.symlink(self.hps_fieldmaps_dir, symlink_name)
            print('Created fieldmap symlink to dir: ' + self.hps_fieldmaps_dir)
