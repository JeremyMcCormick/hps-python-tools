import luigi
import json

from hps.batch.examples import ExampleTask

class JSONWriter:

    def __init__(self, task, json_file_name):
        self.task = task
        self.json_file_name = json_file_name
                    
    def to_dict(self):
        task_name = type(self.task).__name__
        module = self.task.__class__.__module__
        data = {}
        data['Module'] = module
        data['Task'] = task_name
        parameters = self.parameters()
        data['Parameters'] = parameters        
        return data
        
    def write(self):
        data = self.to_dict()
        with open(self.json_file_name, 'w') as jsonout:
            json.dump(data, jsonout, indent=4)
        print("Wrote JSON data to '%s'" % self.json_file_name)
    
    def parameters(self):
        tasks = [self.task]
        d = {}
        done = False
        while(not done):
            self.task_parameters(tasks[0], d)
            tasks += luigi.task.flatten(tasks[0].requires())
            tasks.pop(0)
            if len(tasks) == 0:
                done = True
        return d

    def task_parameters(self, task, d):
        for k,v in task.param_kwargs.iteritems():
            if isinstance(v, tuple):
                v = list(v)
            elif isinstance(v, luigi.parameter._FrozenOrderedDict):
                dict_param = {}
                for param_k, param_v in v.iteritems():
                    dict_param[param_k] = param_v
                v = dict_param
            if task != self.task:
                k = '%s-%s' % (task.__class__.__name__, k)
            d[k] = v
                
if __name__ == '__main__':
    task = ExampleTask()
    luigi.build([task], workers=0, local_scheduler=True)
    JSONWriter(task, 'example_task.json').write()