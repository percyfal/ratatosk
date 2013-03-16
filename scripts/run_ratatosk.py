import luigi
import os
import sys
import ratatosk.lib.align.bwa
import ratatosk.lib.tools.gatk
import ratatosk.lib.tools.samtools
import ratatosk.lib.tools.picard
from ratatosk.scilife.seqcap import HaloPlex
from ratatosk.scilife import config_dict

if __name__ == "__main__":
    if len(sys.argv) > 1:
        task = sys.argv[1]
    else:
        task = None
    if task == "HaloPlex":
        args = sys.argv[2:] + ['--config-file', config_dict['haloplex']]
        luigi.run(args, main_task_cls=ratatosk.scilife.seqcap.HaloPlex)
    # Add more standard pipelines here...
    elif task == "Exome":
        pass
    # Whatever other task/config the user wants to run
    else:
        luigi.run()
