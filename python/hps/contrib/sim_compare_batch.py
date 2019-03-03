#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sun Mar  3 11:34:38 2019

@author: jermc
"""

import luigi
import os, subprocess

class SlicTask(luigi.Task):
    
    geom = luigi.Parameter(default="HPS-EngRun2015-Nominal-v3-5-3-fieldmap.lcdd")
    mac = luigi.Parameter(default="slic_gun.mac")
    output_file = luigi.Parameter(default="slicEvents.slcio")
    
    def run(self):

        #slic -g ./HPS-EngRun2015-Nominal-v3-5-3-fieldmap.lcdd -m slic_gun.mac
        
        cmd = ['slic', '-g', self.geom, '-m', self.mac]
        print "running " + str(cmd)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        for l in p.stdout:
            print l
        p.wait()
        print p.returncode
        
    def output(self):
        return luigi.LocalTarget(self.output_file)

#class SimCompareTask():

if __name__ == "__main__":
    luigi.run()
    