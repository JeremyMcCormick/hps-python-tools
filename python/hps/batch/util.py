"""
Utilities for batch jobs.

@author: Jeremy McCormick (SLAC)
"""

import subprocess

def run_process(c, use_shell=False):
    if isinstance(c, basestring):
        cmd = c.split()
    elif isinstance(c, tuple) or isinstance(c, list):
        cmd = c
        if use_shell:
            cmd = " ".join(cmd)
    else:
        raise Exception("Bad command argument to run_process: %s" % c)
    print("Running command: %s" % cmd)
    print("Using shell: %s" % str(use_shell))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=use_shell)
    print("OUTPUT:")
    for l in p.stdout.readlines():
        print(l.strip())
    print()
    print("ERRORS: ")
    for l in p.stderr.readlines():
        print(l.strip())
    print()
    p.wait()
    print("returncode: " + str(p.returncode))
