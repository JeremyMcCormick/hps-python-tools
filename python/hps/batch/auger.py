from string import Template
import luigi
import socket, getpass, os

from hps.batch.writer import JSONTask
from hps.batch.examples import ExampleTask

auger_tmpl = """<Request>
<Email email="${user}" request="false" job="false"/>
<Project name="hps"/>
<Track name="simulation"/>
<Name name="${name}"/>
<Command><![CDATA[
${command}
]]></Command>
<Job>
<Input src="${input_src}" dest="${input_dest}"/>
<Output src="${output_src}" dest="${output_dest}"/>
<Stderr dest="${stderr}"/>
<Stdout dest="${stdout}"/>
</Job>
</Request>"""

class AugerWriter:
    
    def __init__(self, outfile = 'auger.xml', parameters = {}):
        self.outfile = outfile
        self.parameters = parameters
        
    def write(self):
        tmpl = Template(auger_tmpl)
        print(repr(tmpl))
        subs = tmpl.substitute(self.parameters)
        print(subs)
        with open(self.outfile, 'w') as augerout:
            augerout.write(subs)
            
class AugerTask(luigi.Task):
    
    auger_file = luigi.Parameter(default='auger.xml')
    job_name = luigi.Parameter(default='MyJob')
    check_host = luigi.BoolParameter(default=False)
    output_src = luigi.Parameter()
    output_dest = luigi.Parameter()
    
    def __init__(self, *args, **kwargs):
        super(AugerTask, self).__init__(*args, **kwargs)
        self.task = None

    def set_task(self, task):
        self.task = task
           
    def requires(self):
        json = JSONTask()
        json.set_task(self.task)
        return json
    
    def run(self):
        if self.task is None:
            raise Exception('AugerTask requires luigi task to be set.')
        self.json_file = self.input()
        if self.check_host:
            if 'jlab.org' not in socket.gethostname():
                raise Exception('Host is not at JLab!')
        p = {
                'user': '%s@jlab.org' % getpass.getuser(),
                'name': self.job_name,
                'command': 'cmdline.py %s' % self.json_file.path,
                'input_src': self.json_file.path,
                'input_dest': os.path.basename(self.json_file.path),
                'output_src': self.output_src,
                'output_dest': self.output_dest,
                'stderr': 'stderr.log',
                'stdout': 'stdout.log'         
        }
        writer = AugerWriter(parameters=p)
        writer.write()
                        
    def output(self):
        return luigi.LocalTarget(self.auger_file)

if __name__ == '__main__':
    task = AugerTask(output_src='output.txt', output_dest='/dummy/output.txt')
    task.set_task(ExampleTask())
    luigi.build([task], workers=1, local_scheduler=True)