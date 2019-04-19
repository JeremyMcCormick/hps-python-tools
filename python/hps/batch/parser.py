import luigi
import json

class DummyTask(luigi.Task):

    param1 = luigi.Parameter(default="boggle")
    
    def run(self):
        print("Hello DummyTask!")
        print("param1 = %s" % self.param1)
        open('DummyTaskComplete.x', 'w').close()
        
    def output(self):
        return luigi.LocalTarget('DummyTaskComplete.x')

class ExampleTask(luigi.Task):
    
    param1 = luigi.Parameter(default="foobarbaz")
    param2 = luigi.IntParameter(default=1234)
    param3 = luigi.FloatParameter(default=56.78)
    param4 = luigi.BoolParameter(default=False)
    
    # '{"role": "web", "env": "staging"}'
    param5 = luigi.DictParameter()
    
    def run(self):
        print("Hello ExampleTask!")
        print("param1 = %s" % self.param1)
        print("param2 = %s" % self.param2)
        print("param3 = %s" % self.param3)
        print("param4 = %s" % self.param4)
        print("param5 = %s" % self.param5)
        open('ExampleTaskComplete.x', 'w').close()
        
    def requires(self):
        return DummyTask()
    
    def output(self):
        return luigi.LocalTarget('ExampleTaskComplete.x')

def get_str_val(raw_value):
    if isinstance(raw_value, basestring):
        return raw_value.lstrip('u').lstrip("'").rstrip("'").encode('ascii', 'ignore')
    else:
        return str(raw_value).encode('ascii', 'ignore')
            
class JSONParser:

    path = None
    cmds = []

    def __init__(self, path):
        self.path = path

    def parse(self):
        with open(self.path) as json_file:
            data = json.load(json_file)
            luigi_node = data['Luigi']
            cmd = []
            for task_node in luigi_node:
                cmd.append(task_node['Task'].encode('ascii', 'ignore'))
                param_node = task_node['Parameters']
                for key, value in param_node.iteritems():
                    if not (isinstance(value, bool) and value == False):
                        cmd.append(("--%s" % key).encode('ascii', 'ignore'))
                    if not isinstance(value, bool):
                        if isinstance(value, dict):
                            dict_str = "{"
                            for dict_param_key, dict_param_value in value.iteritems():
                                dict_str += "\"%s\":" % get_str_val(dict_param_key)    
                                if isinstance(dict_param_value, basestring):
                                    dict_str += "\"%s\"" % get_str_val(dict_param_value)
                                else:
                                    dict_str += get_str_val(dict_param_value)
                                dict_str += ","
                            dict_str = dict_str[:-1]
                            dict_str += "}"
                            cmd.append(dict_str)
                        elif isinstance(value, basestring):
                            cmd.append(get_str_val(value))
                        else:
                            cmd.append(get_str_val(value))
                self.cmds.append(cmd)
            return self.cmds
    
json_example = 'example.json'

# luigi.run(['examples.HelloWorldTask', '--workers', '1', '--local-scheduler'])   
if __name__ == '__main__':
    cmds = JSONParser(json_example).parse()
    for cmd in cmds:
        cmd.extend(['--workers', '1', '--local-scheduler'])
        print("Running command: %s" % cmd)
        luigi.run(cmd)
        