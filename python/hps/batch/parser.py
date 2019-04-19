import luigi
import json, sys

def get_str_val(raw_value):
    if isinstance(raw_value, basestring):
        return raw_value.lstrip('u').lstrip("'").rstrip("'").encode('ascii', 'ignore')
    else:
        return str(raw_value).encode('ascii', 'ignore')
            
class JSONParser:

    """Parse a JSON file containing a description of a Luigi task and parameters.
    
    For example, this JSON text describes a job using the example classes from hps.batch.examples::
    
        {
            "Luigi": [
                {
                    "Module": "hps.batch.examples",
                    "Task": "ExampleTask",
                    "Parameters": {
                        "param1": "turkey",
                        "param2": 5678,
                        "param3": 3.14,
                        "param4": true,
                        "param5": {
                            "key1": "Drogo",
                            "key2": "Dragons",
                            "key3": 1234,
                            "key4": 12.34
                        },
                        "DummyTask-param1": "dingus"
                    }
                }
            ]
        }    
    
    The "Module", "Task", and "Parameter" arguments are all required (for now).
    
    """
    
    path = None
    cmds = []

    def __init__(self, path):
        self.path = path

    def parse(self):
        """This is a somewhat fugly method to convert JSON into a list of tasks to run.
        
        The JSON is returned by Python as unicode, so we have to strip out a bunch of prepends and convert to ASCII 
        in order to run in Luigi.
        
        DictParameter is generally handled correctly, but no nested dictionaries are allowed.  Boolean parameters within
        the dictionaries (true or false) in the JSON do not seem to be parsed correctly by Luigi even though they are part 
        of the JSON standard, so they are not allowed.  BoolParameter objects seem to work fine though.
        """
        with open(self.path) as json_file:
            data = json.load(json_file)
            luigi_node = data['Luigi']
            cmd = []
            for task_node in luigi_node:
                cmd.append(task_node['Task'].encode('ascii', 'ignore'))
                cmd.append('--module')
                cmd.append(task_node['Module'].encode('ascii', 'ignore'))
                param_node = task_node['Parameters']
                for key, value in param_node.iteritems():
                    # setting a boolean parameter to false via the command line is not allowed due to how Luigi works
                    if not (isinstance(value, bool) and value == False):
                        cmd.append(("--%s" % key).encode('ascii', 'ignore'))
                    # bool is turned on by just using the switch with no value
                    if not isinstance(value, bool):
                        # handle dict type
                        if isinstance(value, dict):
                            # build the dict for the command line (ugly!!!)
                            dict_str = "{"
                            for dict_param_key, dict_param_value in value.iteritems():
                                dict_str += "\"%s\":" % get_str_val(dict_param_key)    
                                if isinstance(dict_param_value, basestring):
                                    dict_str += "\"%s\"" % get_str_val(dict_param_value)
                                elif isinstance(dict_param_value, bool):
                                    dict_str += str(dict_param_value).lower()
                                else:
                                    dict_str += get_str_val(dict_param_value)
                                dict_str += ","
                            dict_str = dict_str[:-1]
                            dict_str += "}"
                            cmd.append(dict_str)
                        elif isinstance(value, basestring):
                            # strings are stripped of type prepend and quotes and then appended
                            cmd.append(get_str_val(value))                        
                        else:
                            # other types like float and int are just appended
                            cmd.append(get_str_val(value))
                self.cmds.append(cmd)
            return self.cmds

# luigi.run(['examples.HelloWorldTask', '--workers', '1', '--local-scheduler']) 
if __name__ == '__main__':

    # single argument is JSON file to read
    json_path = 'example.json'
    if len(sys.argv) > 1:
        json_path = sys.argv[1]
    
    # get a list of task descriptions to run
    cmds = JSONParser(json_path).parse()
    
    # loop over tasks and run them sequentially (which is dumb)
    for cmd in cmds:
        cmd.extend(['--workers', '1', '--local-scheduler'])
        print("Running command: %s" % cmd)
        luigi.run(cmd)
        