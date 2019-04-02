import luigi

from hps.batch.config import hps as hps_config
hps_config().setup()

from hps.batch.tasks import FileListTask
from hps.batch.jobs.pulser_plots import PulserPlotsProcessor
from hps.lcio.proc import EventManager

class PulserAnalTask(luigi.Task):
    
    plot_file = luigi.Parameter(default='pulserPlots.root')
    files = luigi.ListParameter(default=['hps_008094.0_pulser_4.2.slcio'])
    
    def run(self):
        processors = [PulserPlotsProcessor(plot_file_name=self.plot_file)]
        #files = [f.path for f in luigi.task.flatten(self.input())]
        files = [f for f in luigi.task.flatten(self.files)]
        mgr = EventManager(processors, files)
        mgr.process() 
    
    #def requires(self):
    #    return FileListTask(files=['hps_008094.0_pulser_4.2.slcio'])
    
    def output(self):
        return luigi.LocalTarget(self.plot_file)
