import json

def get_str_val(raw_value):
    if isinstance(raw_value, basestring):
        return raw_value.lstrip('u').lstrip("'").rstrip("'").encode('ascii', 'ignore')
    else:
        return str(raw_value).encode('ascii', 'ignore')

class JSONParser:

    """Parse a JSON file containing a description of one or more Luigi tasks, returning a list of commands 
    that can be passed to the luigi.run() method.
        
    The "Module", "Task", and "Parameter" arguments are all required (for now).    
    """
    
    path = None
    cmds = []

    def __init__(self, path):
        self.path = path

    def parse(self):
        """This is a somewhat ugly method to convert JSON into a list of tasks to run.
        
        The JSON is returned by Python as unicode, so we have to strip out the type prefix and convert to ASCII 
        in order to run in Luigi.
        
        DictParameter is handled correctly, but no nested dictionaries or lists are allowed within them.
        
        ListParameter is also handled correctly, but with similar restrictions of no nested data.
        """
        cmds = []
        with open(self.path) as json_file:
            data = json.load(json_file)
            if isinstance(data, list):
                # list of tasks
                for task_node in data:
                    cmd = self.create_task(task_node)
                    cmds.append(cmd)
            elif isinstance(data, dict):
                # single task
                cmds.append(self.create_task(data))                
            return cmds
        
    def create_task(self, task_node):
        cmd = []
        cmd.append(task_node['Task'].encode('ascii', 'ignore'))
        cmd.append('--module')
        cmd.append(task_node['Module'].encode('ascii', 'ignore'))
        param_node = task_node['Parameters']
        # loop over the parameter dictionary
        for key, value in param_node.iteritems():
            # setting a boolean parameter to false is not allowed due to how Luigi works :-(
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
                # handle list type
                elif isinstance(value, list):
                    list_str = "["
                    for list_param_value in value:
                        if isinstance(list_param_value, basestring):
                            list_str += "\"%s\"" % get_str_val(list_param_value)
                        elif isinstance(list_param_value, bool):
                            list_str += str(list_param_value).lower()
                        else:
                            list_str += get_str_val(list_param_value)
                        list_str += ","
                    list_str = list_str[:-1]
                    list_str += "]"
                    cmd.append(list_str)
                elif isinstance(value, basestring):
                    # strings are stripped of type prefix and quotes and then appended
                    cmd.append(get_str_val(value))
                else:
                    # other types like float and int are just appended
                    cmd.append(get_str_val(value))
        return cmd

# luigi.run(['examples.HelloWorldTask', '--workers', '1', '--local-scheduler']) 
"""            
if __name__ == '__main__':

    # single argument is JSON file to read
    json_path = 'example.json'
    if len(sys.argv) > 1:
        json_path = sys.argv[1]
    
    # get a list of task descriptions to run
    cmds = JSONParser(json_path).parse()
    
    # loop over list of tasks and run them sequentially (which is pretty dumb)
    for cmd in cmds:
        cmd.extend(['--workers', '1', '--local-scheduler'])
        print("Running command: %s" % cmd)
        luigi.run(cmd)
"""     