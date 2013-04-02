import luigi
import os
import sys
import itertools
from ratatosk import backend
from ratatosk.config import setup_config
from ratatosk.handler import setup_global_handlers
from ratatosk.utils import opt_to_dict, dict_to_opt
import ratatosk.lib.align.bwa
import ratatosk.lib.tools.gatk
import ratatosk.lib.tools.samtools
import ratatosk.lib.tools.picard
from ratatosk.pipeline.haloplex import HaloPlex
from ratatosk.pipeline.align import AlignSeqcap
from ratatosk.pipeline import config_dict

if __name__ == "__main__":
    task_cls = None
    opt_dict = {}
    if len(sys.argv) > 1:
        task = sys.argv[1]
        opt_dict = opt_to_dict(sys.argv[1:])
    else:
        task = None

    # Set task_cls and the corresponding config file
    if task == "HaloPlex":
        opt_dict['--config-file'] = config_dict['haloplex']
        task_cls = ratatosk.pipeline.haloplex.HaloPlex
    elif task == "AlignSeqcap":
        opt_dict['--config-file'] = config_dict['seqcap']
        task_cls = ratatosk.pipeline.align.AlignSeqcap

    setup_config(config_file=opt_dict.get("--config-file"), custom_config_file=opt_dict.get("--custom-config"))
    setup_global_handlers()

    if task_cls:
        task_args = dict_to_opt(opt_dict)
        luigi.run(task_args, main_task_cls=task_cls)
    else:
        # Whatever other task/config the user wants to run
        luigi.run(args)

    # Finally print global configuration
    
