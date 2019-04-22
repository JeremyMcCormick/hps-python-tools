#!/usr/bin/env python

import sys
import luigi
from hps.batch.parser import JSONParser

if __name__ == '__main__':

    # single argument is JSON file to read
    json_path = 'example.json'
    if len(sys.argv) > 1:
        json_path = sys.argv[1]
    else:
        print("Usage: \ncmdline.py jobs.json\n")
        raise Exception("No JSON file was provided!")
    
    # get a list of task descriptions to run
    cmds = JSONParser(json_path).parse()
    
    print(cmds)
    
    # loop over list of tasks and run them sequentially (which is pretty dumb)
    for cmd in cmds:
        cmd.extend(['--workers', '1', '--local-scheduler'])
        print("Running command: %s" % cmd)
        luigi.run(cmd)