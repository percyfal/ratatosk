import os
import ratatosk
# Define configuration file  locations and classes for predefined workflows
config_dict = {
    'AlignSeqcap' : {'config' :os.path.join(ratatosk.__path__[0], os.pardir, "config", "seqcap.yaml"),
                     'cls' : ratatosk.pipeline.haloplex.HaloPlex},
    'HaloPlex' : {'config' : os.path.join(ratatosk.__path__[0], os.pardir, "config", "haloplex.yaml")
                  'cls' : ratatosk.pipeline.align.AlignSeqcap},
    }
