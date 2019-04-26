import luigi
import os, glob
import MySQLdb

from hps.batch.config import hps as hps_config
hps_config().setup()

from hps.batch.config import job as job_config

from hps.batch.tasks import EvioToLcioBaseTask

# TODO:
# - tasks to aggregate the ROOT files by run number


class EvioFileUtility:
    """EVIO file utility to get various information from the file name."""
    
    def __init__(self, evio_file_path):
        self.evio_file_path = evio_file_path
        self.basename = os.path.basename(evio_file_path)
    
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
        print(qry)
        self.cur.execute(qry)
        
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
        
    def update(self, ID, recon_file_path, dqm_file_path):
        qry = "update pipeline set id = %d, recon_file_path = '%s', dqm_file_path = '%s')" % (ID, recon_file_path, dqm_file_path)
        self.cur.execute(qry)
    
    def submit(self, ID, batch_id):
        qry = "update pipeline set batch_id = %d where id = %d" % (ID, batch_id)
        self.cur.execute(qry)

    def submitted(self, ID):
        qry = "select batch_id from pipeline where id = %d" % (ID)
        self.cur.execute(qry)
        return self.cur.fetchall()[0][0] is not None;
        
    def close(self, commit=True):
        if commit:
            self.conn.commit()
        self.conn.close()
        
class ReconAndDQMTask(luigi.Task):
    """Task to run the recon and DQM in one job."""
    
    evio_file = luigi.Parameter()
    output_file = luigi.Parameter()
    detector = luigi.Parameter(job_config().detector)
    steering = luigi.Parameter(default='Run2016ReconPlusDataQuality.lcsim') # FIXME: default should be steering resource in git
    output_dir = luigi.Parameter(default=os.getcwd())
    nevents = luigi.Parameter(default=10)
    
    def run(self):
        evio_file_info = EvioFileUtility(self.evio_file)
        yield EvioToLcioBaseTask(evio_files=[self.evio_file],
                                 detector=self.detector,
                                 output_file=self.output_file,
                                 steering=self.steering,
                                 run_number=evio_file_info.run_number(), 
                                 nevents=self.nevents)
        
    def output(self):
        return (luigi.LocalTarget('%s.slcio' % self.output_file), 
                luigi.LocalTarget('%s.root' % self.output_file))
        
# TODO: 
# - Only scan for files newer than a date parameter
# - Date parameter from database
# - Make check of db for existing record a separate task
class DQMPipelineTask(luigi.WrapperTask):
    """Top level task to run the pipeline."""
    
    evio_dir = luigi.Parameter(default=os.getcwd())
    
    detector = luigi.Parameter(job_config().detector)
    steering = luigi.Parameter(default='Run2016ReconPlusDataQuality.lcsim')
    
    output_dir = luigi.Parameter(default=os.getcwd())
            
    def run(self):
        evio_glob = glob.glob('%s/*.evio.*' % (self.evio_dir))
               
        print("EVIO files:")
        print(evio_glob)
        
        db = DQMPipelineDatabase()
        
        for e in evio_glob:
            
            evio_file = EvioFileUtility(e)            
            run_number = evio_file.run_number()
            seq = evio_file.seq()
  
            print("Processing (file, run_number, seq) = ('%s', %d, %d)" % (e, run_number, seq))

            exists = db.exists(run_number, seq)
            ID = None
            if exists:
                ID = db.get_id(run_number, seq)
            submitted = False
            if not exists:
                print("Inserting new EVIO file '%s' into db ..." % e)
                db.insert(e, run_number, seq) # TODO: get id from insert
                db.commit()
                ID = db.get_id(run_number, seq)
            else:
                print("EVIO file already exists so checking if submitted ...")                
                submitted = db.submitted(ID)
                            
            print("EVIO file has ID %d" % ID)
            
            if not exists or (exists and not submitted):
                                
                print("Executing DQM job for '%s' ..." % e)
                                
                recon_name = evio_file.recon_name()
                batch_id = 0 # FIXME: dummy batch ID
                db.submit(ID, batch_id)
                db.commit()
                yield ReconAndDQMTask(evio_file=e,
                                      output_file='%s/%s' % (self.output_dir, recon_name))
                
                print("Done running DQM for '%s'" % (e))

                recon_file_name = recon_name + '.slcio'
                dqm_file_name = recon_name + '.root'
                
                if os.path.exists(recon_file_name) and os.path.exists(dqm_file_name):
                    print("Batch job was successful!")
                    db.update(ID, recon_file_name, dqm_file_name)
                    db.commit()
                else:
                    print("Recon or DQM file is missing after batch job!")
                    # TODO: reset batch_id to null here to indicate it needs resubmission
            else:
                print("EVIO file %d in run %d is already submitted!" % (seq, run_number))
                  
        db.close()
                
    def complete(self):
        return False
    
