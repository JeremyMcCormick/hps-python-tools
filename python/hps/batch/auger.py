from string import Template
import luigi
import socket, getpass

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
        print("PARAMETERS: %s" % str(self.parameters))
        
    def write(self):
        tmpl = Template(auger_tmpl)
        print(repr(tmpl))
        subs = tmpl.substitute(self.parameters)
        print(subs)
        with open(self.outfile, 'w') as augerout:
            augerout.write(subs)
            
class AugerTask(luigi.Task):
    
    dryrun = luigi.BoolParameter(default=False)
    #json_file = luigi.Parameter(default='task.json')
    auger_file = luigi.Parameter(default='auger.xml')
    job_name = luigi.Parameter(default='MyJob')
    #task = luigi.Parameter(default='hps.batch.examples.ExampleTask')
      
    def requires(self):
        return JSONTask()
    
    def run(self):
        self.json_file = self.input()
        #if 'jlab.org' not in socket.gethostname():
        #    raise Exception('Host is not at JLab!')
        #luigi.build([self.task], workers=0, local_scheduler=True)
        p = {
                'user': '%s@jlab.org' % getpass.getuser(),
                'name': self.job_name,
                'command': 'cmdline.py %s' % self.json_file.path,
                'input_src': '/dummy/input.txt',
                'input_dest': 'input.txt',
                'output_src': 'output.txt',
                'output_dest': '/dummy/output.txt',
                'stderr': 'stderr.log',
                'stdout': 'stdout.log'            
        }    
        writer = AugerWriter(parameters=p)
        writer.write()
        
    def output(self):
        return luigi.LocalTarget(self.auger_file)

if __name__ == '__main__':
    luigi.build([AugerTask()], workers=1, local_scheduler=True)