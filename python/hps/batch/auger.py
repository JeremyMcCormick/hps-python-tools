from string import Template
import luigi
import getpass, os, subprocess

from hps.batch.writer import JSONTask
from hps.batch.examples import ExampleTask

auger_tmpl = """<Request>
<Email email="${user}" request="false" job="false"/>
<Project name="hps"/>
<Track name="${track}"/>
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
    
    def __init__(self, tmpl = auger_tmpl, outfile = 'auger.xml', parameters = {}):
        self.tmpl = tmpl
        self.outfile = outfile
        self.parameters = parameters
        
    def write(self):
        tmpl = Template(self.tmpl)
        #print(repr(tmpl))
        subs = tmpl.substitute(self.parameters)
        #print(subs)
        with open(self.outfile, 'w') as augerout:
            augerout.write(subs)
            
class AugerXMLTask(luigi.Task):
    
    auger_file = luigi.Parameter(default='auger.xml')
    #check_host = luigi.BoolParameter(default=False)
    output_src = luigi.Parameter()
    output_dest = luigi.Parameter()
    track = luigi.Parameter(default='debug')
    
    def __init__(self, *args, **kwargs):
        super(AugerXMLTask, self).__init__(*args, **kwargs)
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
        #if self.check_host:
        #    if 'jlab.org' not in socket.gethostname():
        #        raise Exception('Host is not at JLab!')
        p = {
                'user': '%s@jlab.org' % getpass.getuser(),
                'track': self.track,
                'name': self.task.__class__.__name__,
                'command': 'cmdline.py %s' % self.json_file.path,
                'pythonpath': os.getenv('PYTHONPATH'),
                'ldpath': os.getenv('LD_LIBRARY_PATH'),
                'path': os.getenv('PATH'),
                'input_src': os.path.abspath(self.json_file.path),
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

class AugerSubmitTask(luigi.Task):

    submit = luigi.BoolParameter(default=True)
    
    def set_task(self, task):
        self.task = task
        
    def requires(self):
        task = AugerXMLTask()
        task.set_task(self.task)
        return task
    
    """
    jsub -xml auger.xml
    Parsing script ... (it may take while)
    <jsub><request><index>31211294</index><jobIndex>66331253</jobIndex></request></jsub>
    """
    def run(self):
        for i in luigi.task.flatten(self.input(self)):
            cmd = ['jsub', '-xml', i.path]
            print("Auger cmd: %s" % ' '.join(cmd))
            if self.submit:
                p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
                job_id = -1
                for l in p.stdout:
                    l = l.decode().strip()
                    if "<jsub>" in l:
                        job_id = int(l[l.find('<jobIndex>')+10:l.find('</jobIndex>')])
                print("Submitted '%s' with job ID %d" % (str(cmd), job_id))
            else:
                print("Job not submitted!")

if __name__ == '__main__':
    task = AugerSubmitTask(submit=False)
    task.set_task(ExampleTask())
    luigi.build([task], workers=1, local_scheduler=True)