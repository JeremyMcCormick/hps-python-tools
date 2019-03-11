#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 11 13:19:59 2019

@author: jermc
"""

import subprocess

def run_process(c):
    print "Running: " + str(c)
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
