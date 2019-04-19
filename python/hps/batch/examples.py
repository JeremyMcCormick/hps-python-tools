import luigi

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