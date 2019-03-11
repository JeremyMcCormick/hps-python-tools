#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 11 13:30:37 2019

@author: jermc
"""

import luigi

class hps(luigi.Config):
    slic_setup_script = luigi.Parameter()
    sim_setup_script = luigi.Parameter()

#        print hps().slic_setup_script
#        print hps().sim_setup_script
