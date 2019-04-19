import luigi
import json

class ExampleTask(luigi.Task):
    
    param1 = luigi.Parameter(default="foobarbaz")
    param2 = luigi.IntParameter(default=1234)
    param3 = luigi.FloatParameter(default=56.78)
    param4 = luigi.BoolParameter(default=False)
    
    # '{"role": "web", "env": "staging"}'
    #param5 = luigi.DictParameter()
    """
                "param5": {
                "param6": "Drogo",
                "param7": 42
            },
    """
    
    def run(self):
        print("Hello ExampleTask!")
        print("param1 = %s" % self.param1)
        print("param2 = %s" % self.param2)
        print("param3 = %s" % self.param3)
        print("param4 = %s" % self.param4)
        #print("param5 = %s" % self.param5)
        print()
        
    #def requires(self):
    #    return DummyTask()
        
class DummyTask(luigi.Task):

    param1 = luigi.Parameter(default="boggle")
    
    def run(self):
        print("Hello DummyTask!")
        print("param1 = %s" % self.param1)
    
class JSONParser:

    path = None
    cmd = []

    def __init__(self, path):
        self.path = path

    def parse(self):
        with open(self.path) as json_file:
            data = json.load(json_file)
            luigi_node = data['Luigi']
#            .encode('ascii', 'ignore')
            self.cmd.append(luigi_node['Task'].encode('ascii', 'ignore'))
            param_node = luigi_node['Parameters']
            for key, value in param_node.iteritems():
                if not (isinstance(value, bool) and value == False):
                    self.cmd.append(("--%s" % key).encode('ascii', 'ignore'))
                if not isinstance(value, bool):
                    if isinstance(value, dict):
                        print('dict')
                        self.cmd.append((repr(value)))
                    elif isinstance(value, basestring):                      
                        print('str')
                        self.cmd.append(value.lstrip('u').lstrip("'").rstrip("'").encode('ascii', 'ignore'))
                    else:
                        print('stuff')
                        self.cmd.append(str(value).encode('ascii', 'ignore'))
        print self.cmd
        return self.cmd
    
json_example = 'example.json'

if __name__ == '__main__':
    cmd = JSONParser(json_example).parse()
    cmd.extend(['--workers', '1', '--local-scheduler'])
    #print "Running cmd: " + repr(cmd)
    luigi.run(cmd)
        
#    luigi.run(['examples.HelloWorldTask', '--workers', '1', '--local-scheduler'])
    
"""
,
            "DummyTask-param1": "dingus"
"""
    