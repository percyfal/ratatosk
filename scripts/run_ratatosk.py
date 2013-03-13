import luigi
import os
import sys
import ratatosk.bwa
import ratatosk.gatk
import ratatosk.samtools
import ratatosk.picard
from ratatosk.scilife.seqcap import SeqCap

# Idea: run all analyses from here. If a parameter --analysis is
# given, set the config file to a predefined config provided by
# ratatosk. Problem: --config-file is a global parameter in luigi, and
# cannot be changed except via command line. One solution lies in
# modifying luigi.run
config_dict = {
    'seqcap' : os.path.join(ratatosk.__path__[0], os.pardir, "config", "seqcap.yaml"),
    }

if __name__ == "__main__":
    task = sys.argv[1]
    if task == "SeqCap":
        luigi.run(sys.argv[2:], main_task_cls=ratatosk.scilife.seqcap.SeqCap)
    # Add more standard pipelines here...
    elif task == "Halo":
        pass
    # Whatever other task/config the user wants to run
    else:
        luigi.run()
