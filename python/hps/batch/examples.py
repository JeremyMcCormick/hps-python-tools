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
    param5 = luigi.ListParameter(default=["foo", "bar", "baz"])
    
    param6 = luigi.DictParameter(default={"hinkle": "dinkle"})
    
    def requires(self):
        return DummyTask()
        
    def run(self):
        print(">>>> ExampleTask.run")
        print("    param1 = %s" % self.param1)
        print("    param2 = %s" % self.param2)
        print("    param3 = %s" % self.param3)
        print("    param4 = %s" % self.param4)
        print("    param5 = %s" % str(self.param5))
        print("    param6 = %s" % str(self.param6))
        open('%s.done' % self.task_id, 'w').close()
            
    def output(self):
        return luigi.LocalTarget('%s.done' % self.task_id)
            
class DummyMultiTask(luigi.WrapperTask):
    
    def requires(self):
        return [ExampleTask(param1='dingus'), ExampleTask(param2='fingus')]

class YieldTestTask(luigi.Task):

    ntasks = luigi.IntParameter(default=2)    
            
    def run(self):
        for i in range(self.ntasks):
            print('yielding task %d' % i)
            yield ExampleTask(param1='task%d' % i)
            print('done yielding task %d' % i)
        
    def output(self):
        return [luigi.LocalTarget('YieldTestTask%d.done' % i) for i in range(self.ntasks)]
        
class YieldListTestTask(luigi.Task):
    
    def __init__(self, *args, **kwargs):
        super(YieldListTestTask, self).__init__(*args, **kwargs)
        self.ran = False
    
    def run(self):
        print('>>> YieldListTestTask.run')
        print('yielding tasks')
        yield (ExampleTask(param1='fuckin'), ExampleTask(param1='shit'))
        print('DONE yielding tasks')
        self.output().open('w').close()

    def output(self):
        return luigi.LocalTarget('YieldListTestTask.done')
        
class SimpleWrapperTask(luigi.WrapperTask):

    def __init__(self, *args, **kwargs):
        super(SimpleWrapperTask, self).__init__(*args, **kwargs)

    def requires(self):
        return None
    
    def run(self):
        print('>>>> SimpleWrapperTask.run')
        
    def output(self):
        return luigi.LocalTarget('herpderp.txt')
        