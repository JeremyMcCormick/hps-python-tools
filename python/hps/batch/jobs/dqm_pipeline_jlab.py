import luigi
import os, glob, shutil, getpass, logging, subprocess, datetime
import MySQLdb

from hps.batch.config import job as job_config
from hps.batch.config import dqm as dqm_config

from hps.batch.util import run_process
from hps.batch.auger import AugerWriter

class EvioFileUtility:
    """EVIO file utility to get various information from the file name."""

    def __init__(self, evio_file_path):
        self.path = evio_file_path
        self.basename = os.path.basename(self.path)

    def seq(self):
        return int(os.path.splitext(self.basename)[1][1:])

    def run_number(self):
        return int(os.path.splitext(self.basename)[0][4:10])

    def basename_noext(self):
        return os.path.splitext(self.basename)[0].replace('.evio', '')

    def basename(self):
        return self.basename

    def recon_name(self):
        return '%s-%d_recon' % (self.basename_noext(), self.seq())

    def dqm_name(self):
        return '%s-%d_dqm' % (self.basename_noext(), self.seq())

class DQMPipelineDatabase:
    """Interface to the pipeline database."""

    user = dqm_config().user
    passwd = dqm_config().passwd
    db = dqm_config().db
    url = dqm_config().url

    def __init__(self):
        self.conn = MySQLdb.connect(host=self.url, user=self.user, passwd=self.passwd, db=self.db)
        self.cur = self.conn.cursor()

    def insert(self, evio_file_path, run_number, file_seq):
        qry = "insert into pipeline (run_number, evio_file_path, file_seq) values (%d, '%s', %d)" % (run_number, evio_file_path, file_seq)
        self.cur.execute(qry)
        return self.conn.insert_id()

    def exists(self, run_number, file_seq):
        qry = "select id from pipeline where run_number = %d and file_seq = %d" % (run_number, file_seq)
        self.cur.execute(qry)
        return len(self.cur.fetchall())

    def commit(self):
        self.conn.commit()

    def submit(self, ID, job_id, dqm_file_path):
        qry = "update pipeline set job_id = %d, dqm_file_path = '%s', job_status = 'A' where id = %d" % (job_id, dqm_file_path, ID)
        self.cur.execute(qry)

    def close(self):
        self.conn.close()

    def find_dqm(self, dqm_file_path):
        qry = "select * from pipeline where dqm_file_path = '%s'" % dqm_file_path
        self.cur.execute(qry)
        return self.cur.fetchall()

    def find_evio(self, evio_file_path):
        qry = "select * from pipeline where evio_file_path = '%s'" % evio_file_path
        self.cur.execute(qry)
        return self.cur.fetchall()

    def error(self, ID, error_msg):
        qry = "update pipeline set error_msg = '%s', job_status = 'E' where id = %d" % (error_msg, ID)
        self.cur.execute(qry)
        
    def aggregate(self, ID):
        qry = "update pipeline set aggregated = 1 where id = %d" % ID
        self.cur.execute(qry)
        
    def jobs(self):
        qry = "select ID, job_id, job_status, dqm_file_path from pipeline where job_id is not null and not (job_status in ('C','E'))"
        self.cur.execute(qry)
        return self.cur.fetchall()
    
    def update_job_status(self, ID, job_status):
        qry = "update pipeline set job_status = '%s' where id = %d" % (job_status, ID)
        self.cur.execute(qry)
        
    def unaggregated(self):
        qry = "select ID, run_number, dqm_file_path from pipeline where job_status = 'C' and aggregated = 0"
        self.cur.execute(qry)
        return self.cur.fetchall()
    
    def no_error(self, ID):
        qry = "select ID from pipeline where id = %d and job_status = 'E'" % ID
        self.cur.execute(qry)
        return len(self.cur.fetchall()) == 0

def dqm_to_evio(dqm_file_name):
    dirname = os.path.dirname(dqm_file_name)
    evio_file_name = os.path.basename(dqm_file_name.replace('.root', '').replace('_dqm',''))
    i = int(evio_file_name.rfind('-') + 1)
    seq = int(evio_file_name[i:])
    evio_file_name = '%s.evio.%d' % (evio_file_name[:10], seq)
    return '%s/%s' % (dirname, evio_file_name)

def run_from_dqm(dqm_file_name):
    basename = os.path.basename(dqm_file_name)
    return int(basename[5:11])

# TODO:
# - output file should be a text file containing a list of new EVIO files named using date params
class EvioFileScannerOldTask(luigi.Task):

    evio_dir = luigi.Parameter(default=os.getcwd())

    # Files must be newer than this date.  Default is a date before HPS existed. :)
    date = luigi.DateParameter(default=datetime.date(1999, 1, 1))
            
    def __init__(self, *args, **kwargs):
        super(EvioFileScannerOldTask, self).__init__(*args, **kwargs)
        self.output_files = []

    def run(self):
        db = DQMPipelineDatabase()

        try:
            evio_glob = glob.glob('%s/*.evio.*' % (self.evio_dir))
            for e in evio_glob:                
                file_date = datetime.date.fromtimestamp(os.path.getmtime(e))
                if file_date >= self.date:                
                    evio_info = EvioFileUtility(e)
                    run_number = evio_info.run_number()
                    seq = evio_info.seq()
                    if not db.exists(evio_info.run_number(), evio_info.seq()):
                        self.output_files.append(e)
                        logging.info("Inserting (file, run_number, seq) = ('%s', %d, %d) into db ..." % (evio_info.path, run_number, seq))
                        db.insert(evio_info.path, run_number, seq)
                        db.commit()
                    else:
                        logging.info("EVIO file '%s' with run %d and seq %d is already in database!" % (evio_info.path, run_number, seq))
                else:
                    logging.debug("Skipping EVIO file '%s' which was created before %s." % (e, self.date))
            if not len(self.output_files):
                logging.warning('No new EVIO files found!')
        finally:
            db.close()
                
    def output(self):
        return [luigi.LocalTarget(o) for o in self.output_files]
    
class EvioFileScannerTask(luigi.Task):

    evio_dir = luigi.Parameter(default=os.getcwd())

    # Files must be newer than this date.  Default is a date before HPS existed. :)
    date = luigi.DateParameter(default=datetime.date(1999, 1, 1))
            
    work_dir = luigi.Parameter(default=os.getcwd())
    
    def __init__(self, *args, **kwargs):
        super(EvioFileScannerTask, self).__init__(*args, **kwargs)
        self.output_files = []

    def run(self):
        db = DQMPipelineDatabase()

        try:
            evio_glob = glob.glob('%s/*.evio.*' % (self.evio_dir))
            with open('%s/%s' % (self.work_dir, self.data_file_name()), 'w+') as outfile:
                for e in evio_glob:
                    file_date = datetime.date.fromtimestamp(os.path.getmtime(e))
                    if file_date >= self.date:                
                        evio_info = EvioFileUtility(e)
                        run_number = evio_info.run_number()
                        seq = evio_info.seq()
                        if not db.exists(evio_info.run_number(), evio_info.seq()):
                            self.output_files.append(e)
                            logging.info("Inserting (file, run_number, seq) = ('%s', %d, %d) into db ..." % (evio_info.path, run_number, seq))
                            db.insert(evio_info.path, run_number, seq)
                            db.commit()
                        else:
                            logging.info("EVIO file '%s' with run %d and seq %d is already in database!" % (evio_info.path, run_number, seq))
                        logging.info("Appending '%s' to '%s'" % (e, outfile.name))
                        outfile.write(e + '\n')
                    else:
                        logging.debug("Skipping EVIO file '%s' which was created before %s." % (e, self.date))
            
            if not len(self.output_files):
                logging.warning('No new EVIO files found!')
        finally:
            db.close()
                
    def output(self):
        return luigi.LocalTarget(self.data_file_name())
    
    def data_file_name(self):
        today = datetime.date.today()
        return 'evio_files_%d-%02d-%02d.txt' % (today.year, today.month, today.day)

auger_tmpl = """<Request>
<Email email="${user}" request="false" job="true"/>
<Project name="hps"/>
<Track name="${track}"/>
<Name name="${jobname}"/>
<OS name="centos7"/>
<Memory space="${memory}" unit="MB"/>
<TimeLimit time="3" unit="hours"/>
<Command><![CDATA[
${command}
]]></Command>
<Job>
<Output src="${output_src}" dest="${output_dest}"/>
<Input src="${jobscript}" dest="jobscript.sh"/>
<Stderr dest="${logdir}/${jobname}.err"/>
<Stdout dest="${logdir}/${jobname}.out"/>
</Job>
</Request>"""

# TODO:
# - input should be file with list of EVIO files (from scanner)
class SubmitEvioJobsTask(luigi.Task):
    """Submits sequentially the Auger batch jobs to run recon and DQM on input EVIO files."""

    detector = luigi.Parameter(job_config().detector)
    steering = luigi.Parameter(default='/org/hps/steering/production/Run2016ReconPlusDataQuality.lcsim')
    output_dir = luigi.Parameter(default=os.getcwd())
    nevents = luigi.IntParameter(default=-1)    
    submit = luigi.BoolParameter(default=False)
    track = luigi.Parameter(default='debug')
    log_dir = luigi.Parameter(default=os.getcwd())
    auger_file = luigi.Parameter(default='auger.xml')    
    email = luigi.Parameter(default=None)    
    work_dir = luigi.Parameter(default=os.getcwd())
    memory = luigi.IntParameter(default=4000)
    
    def requires(self):
        return EvioFileScannerTask()

    def run(self):
        
        db = DQMPipelineDatabase()
        
        try:
            # EvioFileScannerTask returns a text file with a list of EVIO files.
            with open(self.input()) as infile:
                for i in infile.readlines():
                    
                    evio_info = EvioFileUtility(i.decode().strip())
                    ID = db.find_evio(evio_info.path)[0][0]
                    
                    cmdlines = ['#!/usr/bin/bash']
                    cmdlines.append('source %s/bin/activate python3' % dqm_config().conda_dir)
                    cmdlines.append('export PYTHONPATH=%s/python' % dqm_config().hpspythontools_dir)
                    cmdlines.append('export LUIGI_CONFIG_PATH=%s' % dqm_config().luigi_cfg)
                    cmdlines.append(' '.join(['luigi',
                                              '--module hps.batch.tasks',
                                              'EvioToLcioBaseTask',
                                              """--evio-files '["%s"]'""" % evio_info.path,
                                              '--detector %s' % self.detector,
                                              '--output-file %s' % evio_info.dqm_name(),
                                              '--resource',
                                              '--steering %s' % self.steering,
                                              '--run-number %d' % evio_info.run_number(),
                                              '--nevents %d' % self.nevents,
                                              '--output-ext .root',
                                              '--local-scheduler']))
             
                    email = self.email
                    if email == None:
                        email = '%s@jlab.org' % getpass.getuser()
                    jobname = 'DQM_%06d_%d' % (evio_info.run_number(), evio_info.seq())
                    jobscript = '%s/%s.sh' % (self.work_dir, jobname)
                    with open(jobscript, 'w+') as jobout:
                        for cmd in cmdlines:
                            jobout.write(cmd + '\n')
                    os.chmod(jobscript, 0o755)
                                        
                    parameters = {
                            'user': email,
                            'track': self.track,
                            'jobname': jobname,
                            'jobscript': jobscript,
                            'command': './jobscript.sh',
                            'memory': self.memory,
                            'output_src': '%s.root' % evio_info.dqm_name(),
                            'output_dest': '%s/%s.root' % (self.output_dir, evio_info.dqm_name()),
                            'logdir': self.log_dir
                    }
                    
                    AugerWriter(tmpl=auger_tmpl, parameters=parameters).write()
                    
                    cmd = ['jsub', '-xml', self.auger_file]
                    if self.submit:
                        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
                        job_id = -1
                        for l in p.stdout:
                            l = l.decode().strip()
                            if "<jsub>" in l:
                                job_id = int(l[l.find('<jobIndex>')+10:l.find('</jobIndex>')])
                        if job_id == -1:
                            logging.critical("Failed to submit '%s' batch job!" % evio_info.path)
                            db.error(ID, 'Failed to submit batch job.')
                        else:
                            db.submit(ID, job_id, '%s.root' % evio_info.dqm_name())
                            db.commit()
                            logging.info("Submitted '%s' with job ID %d" % (str(cmd), job_id))
                    else:
                        logging.warning("Job not submitted because submit is set to false!")
                
        finally:
            db.close()
            
    def output(self):
        # FIXME: This output is probably not needed.
        return luigi.LocalTarget(self.auger_file)
            
class AggregateTask(luigi.Task):
    """Task that will aggregate ROOT QDM files into a single output file by run number.
    
    The files for each run number are aggregated using a list of tasks yielded in the run() method.
    """
    
    output_dir = luigi.Parameter(default=os.getcwd())

    def __init__(self, *args, **kwargs):
        super(AggregateTask, self).__init__(*args, **kwargs)
        self.ran = False
        self.output_files = []
        
    def requires(self):
        return UpdateJobStatusTask()
    
    def run(self):
        
        if self.ran:
            return
        
        db = DQMPipelineDatabase()
        dqm_files = {}
        try:
            recs = db.unaggregated()
            for r in recs:
                ID = r[0]
                run_number = r[1]
                dqm_file = r[2]
                if db.no_error(ID):
                    if run_number not in dqm_files:
                        dqm_files[run_number] = []
                    dqm_files[run_number].append(dqm_file)
                    logging.info("Queuing '%s' from run %d for aggregation." % (dqm_file, run_number))
                else:
                    logging.warning("Skipping DQM file '%s' with a job error!" % dqm_file)
        finally:
            db.close()
                
        tasks = []

        for run_number, filelist in dqm_files.items():
            targetfile = '%s/hps_%06d_dqm.root' % (self.output_dir, run_number)
            tasks.append(HistAddTask(run_number=run_number, targetfile=targetfile, dqm_files=filelist))
            self.output_files.append(targetfile)
        self.ran = True
        yield tasks
        
    def complete(self):
        return self.ran
 
class HistAddTask(luigi.Task):
    """Task to run the ROOT 'hadd' utility to aggregate DQM files by run number.
    
    Usage: /sw/root/install/bin/hadd [-f[fk][0-9]] [-k] [-T] [-O] [-a]
            [-n maxopenedfiles] [-cachesize size] [-j ncpus] [-v [verbosity]]
            targetfile source1 [source2 source3 ...]
    """    

    run_number = luigi.IntParameter()
    targetfile = luigi.Parameter()
    dqm_files = luigi.ListParameter([])
    
    def __init__(self, *args, **kwargs):
        super(HistAddTask, self).__init__(*args, **kwargs)
                      
    def run(self):
        
        db = DQMPipelineDatabase()

        try:
            cmd = ['. %s/bin/thisroot.sh && hadd' % dqm_config().root_dir]
            cmd.append(self.targetfile)
            if os.path.exists(self.targetfile):
                logging.debug("Replacing old aggregated file '%s'." % self.targetfile)
                oldtargetfile = '%s.old' % self.targetfile
                shutil.copy(self.targetfile, oldtargetfile)
                os.remove(self.targetfile)
                cmd.append(oldtargetfile)
            cmd.append(' '.join(self.dqm_files))
            run_process(cmd, use_shell=True)
            logging.info("Created aggregated DQM file at '%s' for run %d." % (self.targetfile, self.run_number))
            for f in self.dqm_files:
                rec = db.find_dqm(f)[0]
                db.aggregate(rec[0])
                db.commit()
                logging.info("DQM file '%s' is aggregated." % f)
        finally:
            db.close()
            
    def output(self):
        return luigi.LocalTarget(self.targetfile)
    
class UpdateJobStatusTask(luigi.Task):
    """Task to update the status of the batch jobs in the database and check for job errors."""
    
    auger_statuses = ('C','R','Q','H','A')
 
    dqm_min_size = luigi.IntParameter(default=2e+6)
       
    def __init__(self, *args, **kwargs):
        super(UpdateJobStatusTask, self).__init__(*args, **kwargs)
        self.ran = False
    
    def run(self):
                
        db = DQMPipelineDatabase()
        
        try:
            jobs = db.jobs()
            for job in jobs:
                ID = job[0]
                job_id = job[1]
                status = job[2]
                dqm_file = job[3]
                cmd = 'jobstat -j %d' % job[1]
                logging.debug("Checking job %d with DQM file '%s' and current status '%s'" % (job_id, dqm_file, status))
                p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                outlines = p.stdout.readlines()
                new_status = None
                if len(outlines) > 0:
                    l = outlines[0].decode().strip()
                    if l in UpdateJobStatusTask.auger_statuses:
                        new_status = l
                        if new_status in UpdateJobStatusTask.auger_statuses:
                            if new_status != status:
                                db.update_job_status(ID, new_status)
                                db.commit()
                                logging.info("Updated status of job %d from '%s' to '%s'." % (job_id, status, new_status))
                                status = new_status            
                if status == 'C' or new_status is None:
                    if os.path.exists(dqm_file):  
                        if os.path.getsize(dqm_file) < self.dqm_min_size:
                            db.error(ID, "DQM file size too small after batch job completed.")                            
                        elif status != 'C':
                            db.update_job_status(ID, 'C')                        
                            logging.info("Found valid DQM file '%s' and updated job status to complete." % dqm_file)                        
                    else:
                        if status == 'C':
                            db.error('DQM file does not exist after batch job completed.')
                    db.commit()    
                                                    
        finally:
            db.close()
            
        self.ran = True
        
    def complete(self):
        return self.ran
