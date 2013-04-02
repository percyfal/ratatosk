import luigi
import os
import sys
from ratatosk.config import setup_config
import ratatosk.lib.align.bwa
import ratatosk.lib.tools.gatk
import ratatosk.lib.tools.samtools
import ratatosk.lib.tools.picard
from ratatosk.pipeline.haloplex import HaloPlex
from ratatosk.pipeline.align import AlignSeqcap
from ratatosk.pipeline import config_dict

if __name__ == "__main__":
    if len(sys.argv) > 1:
        task = sys.argv[1]
    else:
        task = None
    if task == "HaloPlex":
        args = sys.argv[2:] + ['--config-file', config_dict['haloplex']]
        luigi.run(args, main_task_cls=ratatosk.pipeline.haloplex.HaloPlex)
    elif task == "AlignSeqcap":
        args = sys.argv[2:] + ['--config-file', config_dict['seqcap']]
        luigi.run(args, main_task_cls=ratatosk.pipeline.align.AlignSeqcap)
    # Whatever other task/config the user wants to run
    else:
        luigi.run()
