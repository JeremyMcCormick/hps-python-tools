from hps.batch.config import hps as hps_config
hps_config().setup()

from hps.batch.tasks import SlicStdhepBaseTask, CopyFilesBaseTask
    
# run slic with stdhep file    
class SlicTask(SlicStdhepBaseTask):
 
    pass

class SlicJob(CopyFilesBaseTask):
    
    def requires(self):
        return SlicTask()