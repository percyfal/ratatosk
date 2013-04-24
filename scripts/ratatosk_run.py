import luigi
import os
import sys
import itertools
from ratatosk import backend
from ratatosk.log import setup_logging
from ratatosk.config import setup_config
from ratatosk.handler import setup_global_handlers
import ratatosk.lib.align.bwa
import ratatosk.lib.tools.gatk
import ratatosk.lib.tools.samtools
import ratatosk.lib.tools.picard
from ratatosk.pipeline.haloplex import HaloPlex, HaloPlexSummary
from ratatosk.pipeline.seqcap import SeqCap, SeqCapSummary
from ratatosk.pipeline.align import Align, AlignSummary
from ratatosk.pipeline import config_dict
from ratatosk.report.sphinx import SphinxReport

if __name__ == "__main__":
    task_cls = None
    if len(sys.argv) > 1:
        task = sys.argv[1]
        task_args = sys.argv[2:]
        if task in config_dict.keys():
            # Reset config-file if present
            if "--config-file" in task_args:
                i = task_args.index("--config-file")
                task_args[i+1] = config_dict[task]['config']
            else:
                task_args.append("--config-file")
                task_args.append(config_dict[task]['config'])
            task_cls = config_dict[task]['cls']
    elif len(sys.argv) == 1:
        luigi.run(['-h'])
    else:
        task = None

    config_file = None
    custom_config_file = None
    if "--config-file" in task_args:
        config_file = task_args[task_args.index("--config-file") + 1]
    if "--custom-config" in task_args:
        custom_config_file = task_args[task_args.index("--custom-config") + 1]

    setup_logging()
    setup_config(config_file=config_file, custom_config_file=custom_config_file)
    setup_global_handlers()

    if task_cls:
        luigi.run(task_args, main_task_cls=task_cls)
    else:
        # Whatever other task/config the user wants to run
        luigi.run()
    
