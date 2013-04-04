import os
import ratatosk
from ratatosk.pipeline import haloplex, align

# Define configuration file  locations and classes for predefined workflows
config_dict = {
    'AlignSeqcap' : {'config' : os.path.join(ratatosk.__path__[0], os.pardir, "config", "seqcap.yaml"),
                     'cls' : haloplex.HaloPlex},
    'HaloPlex' : {'config' : os.path.join(ratatosk.__path__[0], os.pardir, "config", "haloplex.yaml"),
                  'cls' : align.AlignSeqcap},
    }
