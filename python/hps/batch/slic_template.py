#!/usr/bin/env python

import json
import sys, glob, os
from string import Template

class JSONTemplate:
    
    def __init__(self, template_name, json_name):
        self.template_name = template_name
        self.json_name = json_name
    
    def generate(self):
        with open(self.template_name) as jsonfile:
            json_in = json.load(jsonfile)
            stdhep_patterns = json_in['Luigi'][0]['Parameters']['stdhep-files']
            stdhep_files = []
            for stdhep_pattern in stdhep_patterns:
                stdhep_files.extend(glob.glob(stdhep_pattern))
            output_tasks = {}
            output_tasks['Luigi'] = []
            with open(template_name, 'r') as tmpl_file:
                tmpl_data = tmpl_file.read().replace('\n', '')
                tmpl = Template(tmpl_data)
                for stdhep_file in stdhep_files:
                    output_file = 'slic_%s.slcio' % os.path.splitext(stdhep_file)[0]
                    sub = tmpl.substitute(output_file=output_file)
                    str_data = json.loads(sub)
                    str_data['Luigi'][0]['Parameters']['stdhep-files'] = [stdhep_file]
                    output_tasks['Luigi'].append(str_data['Luigi'][0])
                    #print(str_data)  
            print(json.dumps(output_tasks, indent=4, sort_keys=True))
            with open(self.json_name, 'w') as jsonout:
                json.dump(output_tasks, jsonout, indent=4)

if __name__ == "__main__":
    
    if len(sys.argv) > 2:
        template_name = sys.argv[1]
        json_name = sys.argv[2]
    else:
        print('Usage:\nslic_template.py [input_template_name] [output_json_name]\n')
        raise Exception('Not enough arguments!')
    
    tmpl = JSONTemplate(template_name, json_name) 
    tmpl.generate()