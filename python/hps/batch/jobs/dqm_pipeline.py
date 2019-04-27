import luigi
import os, glob, shutil
import logging
import MySQLdb

from hps.batch.config import job as job_config

from hps.batch.tasks import EvioToLcioBaseTask
from hps.batch.util import run_process

"""
Steps:
- scan for EVIO files
- insert db record for new EVIO file
- run recon & DQM job
- update db with DQM file and batch ID
- add DQM output to combined file
- update db aggregated flag
- copy combined file to web dir
"""

"""
Deps:
    
EvioFileScannerTask <- EvioFileProcessorTask <- DatabaseUpdateTask <- HistAddTask <- DQMPipelineTask
"""

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

    user = 'dqm-user'
    passwd = '12345'
    db = 'dqm'
    #url = 'hpsdb.jlab.org'
    url = 'localhost'

    def __init__(self):
        self.conn = MySQLdb.connect(host=self.url, user=self.user, passwd=self.passwd, db=self.db)
        self.cur = self.conn.cursor()

    def insert(self, evio_file_path, run_number, file_seq):
        qry = "insert into pipeline (run_number, evio_file_path, file_seq) values (%d, '%s', %d)" % (run_number, evio_file_path, file_seq)
        self.cur.execute(qry)
        return self.conn.insert_id()

    def evio_file_exists(self, evio_file_path):
        qry = "select id from pipeline where evio_file_path = '%s'" % evio_file_path
        self.cur.execute(qry)
        return len(self.cur.fetchall())

    def dqm_file_exists(self, dqm_file_path):
        qry = "select id from pipeline where dqm_file_path = '%s'" % dqm_file_path
        self.cur.execute(qry)
        return len(self.cur.fetchall())

    def exists(self, run_number, file_seq):
        qry = "select id from pipeline where run_number = %d and file_seq = %d" % (run_number, file_seq)
        self.cur.execute(qry)
        return len(self.cur.fetchall())

    def get_id(self, run_number, file_seq):
        qry = "select id from pipeline where run_number = %d and file_seq = %d" % (run_number, file_seq)
        self.cur.execute(qry)
        return self.cur.fetchall()[0][0]

    def commit(self):
        self.conn.commit()

    def update(self, ID, dqm_file_path):
        qry = "update pipeline set dqm_file_path = '%s' where id = %d" % (dqm_file_path, ID)
        self.cur.execute(qry)

    def submit(self, ID, batch_id):
        qry = "update pipeline set batch_id = %d where id = %d" % (batch_id, ID)
        self.cur.execute(qry)

    def submitted(self, ID):
        qry = "select batch_id from pipeline where id = %d" % (ID)
        self.cur.execute(qry)
        return self.cur.fetchall()[0][0] is not None;

    def delete(self, ID):
        self.cur.execute("delete from pipeline where id = %d" % ID)

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
        qry = "update pipeline set error_msg = '%s' where id = %d" % (error_msg, ID)
        self.cur.execute(qry)
        
    def aggregated(self, ID):
        qry = "update pipeline set aggregated = 1 where id = %d" % ID
        self.cur.execute(qry)

def dqm_to_evio(dqm_file_name):
    dirname = os.path.dirname(dqm_file_name)
    evio_file_name = os.path.basename(dqm_file_name.replace('.root', '').replace('_dqm',''))
    i = int(evio_file_name.rfind('-') + 1)
    seq = int(evio_file_name[i:])
    evio_file_name = '%s.evio.%d' % (evio_file_name[:10], seq)
    return '%s/%s' % (dirname, evio_file_name)

class DQMPipelineTask(luigi.WrapperTask):

    def __init__(self, *args, **kwargs):
        super(luigi.WrapperTask, self).__init__(*args, **kwargs)

    def requires(self):
        yield HistAddTask()

class HistAddTask(luigi.Task):

    """
    Usage: /sw/root/install/bin/hadd [-f[fk][0-9]] [-k] [-T] [-O] [-a]
            [-n maxopenedfiles] [-cachesize size] [-j ncpus] [-v [verbosity]]
            targetfile source1 [source2 source3 ...]
    """

    output_dir = luigi.Parameter(default=os.getcwd())

    def __init__(self, *args, **kwargs):
        super(HistAddTask, self).__init__(*args, **kwargs)
        self.ran = False
        self.output_files = []

    def requires(self):
        return DatabaseUpdateTask()

    def run(self):
        db = DQMPipelineDatabase()
        try:
            dqm_files = {}
            for i in luigi.task.flatten(self.input()):
                dqm_file = i.path
                rec = db.find_dqm(dqm_file)[0]
                run_number = rec[1]
                if run_number not in dqm_files:
                     dqm_files[run_number] = []
                dqm_files[run_number].append(dqm_file)

            # TODO Each of these should probably be a separate task
            for run_number, filelist in dqm_files.iteritems():
                cmd = ['hadd']
                targetfile = '%s/hps_%06d_dqm.root' % (self.output_dir, run_number)
                self.output_files.append(targetfile)
                cmd.append(targetfile)
                if os.path.exists(targetfile):
                    oldtargetfile = '%s.old' % targetfile
                    shutil.copy(targetfile, oldtargetfile)
                    os.remove(targetfile)
                    cmd.append(oldtargetfile)
                cmd.append(' '.join(filelist))
                run_process(cmd, use_shell=True)
                for f in filelist:
                    rec = db.find_dqm(f)[0]
                    db.aggregated(rec[0])
                    db.commit()
                    logging.info("Marked DQM file '%s' as aggregated." % f)
        finally:
            db.close()

        self.ran = True

    def complete(self):
        return self.ran

    def output(self):
        return [luigi.LocalTarget(o) for o in self.output_files]

class DatabaseUpdateTask(luigi.Task):
    """Update database with names of DQM files."""

    def __init__(self, *args, **kwargs):
        super(DatabaseUpdateTask, self).__init__(*args, **kwargs)
        self.ran = False

    def requires(self):
        return EvioFileProcessorTask()

    def run(self):

        db = DQMPipelineDatabase()

        try:
            for i in luigi.task.flatten(self.input()):
                dqm_file_name = i.path
                evio_file_name = dqm_to_evio(dqm_file_name)
                rec = db.find_evio(evio_file_name)[0]
                if os.path.exists(dqm_file_name) and os.path.getsize(dqm_file_name) > 0:
                    logging.info("Batch job created DQM file '%s' OKAY!" % dqm_file_name)
                    db.update(rec[0], dqm_file_name)
                else:
                    logging.error("DQM file '%s' does not exist!" % dqm_file_name)
                    db.error(rec[0], 'DQM file does not exist or is invalid after batch job.')
                db.commit()
        finally:
            db.close()

        self.ran = True

    def output(self):
        return [luigi.LocalTarget(o.path) for o in luigi.task.flatten(self.input())]

    def complete(self):
        return self.ran

class EvioFileProcessorTask(luigi.Task):
    """Runs recon and DQM on list of input EVIO files."""

    detector = luigi.Parameter(job_config().detector)
    steering = luigi.Parameter(default='Run2016ReconPlusDataQuality.lcsim') # FIXME: default should be steering resource in git
    output_dir = luigi.Parameter(default=os.getcwd())
    nevents = luigi.Parameter(default=-1)

    def __init__(self, *args, **kwargs):
        super(EvioFileProcessorTask, self).__init__(*args, **kwargs)
        self.ran = False

    def requires(self):
        return EvioFileScannerTask()

    def run(self):
        if self.ran:
            return
        
        db = DQMPipelineDatabase()
        
        try:
            tasks = []
            for i in luigi.task.flatten(self.input()):
                evio_info = EvioFileUtility(i.path)
                ID = db.find_evio(evio_info.path)[0][0]
                db.submit(ID, 0) # FIXME: dummy batch ID
                db.commit()
                tasks.append(EvioToLcioBaseTask(evio_files=[evio_info.path],
                                                detector=self.detector,
                                                output_file=evio_info.dqm_name(),
                                                steering=self.steering,
                                                run_number=evio_info.run_number(),
                                                nevents=self.nevents,
                                                output_ext='.root'))
            self.ran = True
            yield tasks
        finally:
            db.close()

    def complete(self):
        return self.ran

    def output(self):
        return [luigi.LocalTarget('%s/%s%s' % (self.output_dir,
                                               EvioFileUtility(i.path).dqm_name(),
                                               '.root'))
                for i in luigi.task.flatten(self.input())]

class EvioFileScannerTask(luigi.Task):

    evio_dir = luigi.Parameter(default=os.getcwd())

    def __init__(self, *args, **kwargs):
        super(EvioFileScannerTask, self).__init__(*args, **kwargs)
        self.output_files = []
        self.ran = False

    def run(self):
        db = DQMPipelineDatabase()

        try:
            evio_glob = glob.glob('%s/*.evio.*' % (self.evio_dir))
            for e in evio_glob:
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
            if not len(self.output_files):
                raise Exception('No new EVIO files found!')
        finally:
            db.close()
        self.ran = True

    def output(self):
        return [luigi.LocalTarget(o) for o in self.output_files]

    def complete(self):
        return self.ran