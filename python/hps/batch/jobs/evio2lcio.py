from hps.batch.config import hps as hps_config
hps_config().setup()

from hps.batch.tasks import EvioToLcioBaseTask

class EvioToLcio(EvioToLcioBaseTask):
    pass