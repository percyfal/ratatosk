import os
import ratatosk
# Define configuration file locations for predefined workflows
config_dict = {
    'seqcap' : os.path.join(ratatosk.__path__[0], os.pardir, "config", "seqcap.yaml"),
    'haloplex' : os.path.join(ratatosk.__path__[0], os.pardir, "config", "haloplex.yaml"),
    }
