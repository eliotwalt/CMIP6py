import os
from pathlib import Path

# Paths
CMIP6PY_DIR = Path.home() / ".cmip6py" ; os.makedirs(CMIP6PY_DIR, exist_ok=True)
CACHE_DIR = CMIP6PY_DIR / "cache" ; os.makedirs(CACHE_DIR, exist_ok=True)

# CONFIGS
PYESGF_CONFIG = {
    # List of available index nodes: https://esgf.llnl.gov/nodes.html
    # Be careful about the url, not all search urls have CMIP3 data?
    'urls': [
        'https://esgf.ceda.ac.uk/esg-search',
        'https://esgf-node.llnl.gov/esg-search',
        'https://esgf-data.dkrz.de/esg-search',
        'https://esgf-node.ipsl.upmc.fr/esg-search',
        'https://esg-dn1.nsc.liu.se/esg-search',
        'https://esgf.nci.org.au/esg-search',
        'https://esgf.nccs.nasa.gov/esg-search',
        'https://esgdata.gfdl.noaa.gov/esg-search',
        'https://esgf-node.ornl.gov/esg-search',
    ],
    'distrib': True,
    'timeout': 240,
    'cache': CACHE_DIR / 'pyesgf-search-results',
    'expire_after': 86400,  # cache expires after 1 day
}
ESGF_DOWNLOAD_TIMEOUT = 600 # [s] 10 minutes 

# KEYS
AUTH_KEYS = ["username", "hostname", "password"]

# FACETS
RESULTS_FACETS = [
    "source_id",
    "experiment_id",
    "member_id",
    "variable",
    "grid_label",
    "table_id",
    "version",
    "data_node"
]
RESULTS_FACETS_STRING = ",".join(RESULTS_FACETS)
os.environ["ESGF_PYCLIENT_NO_FACETS_STAR_WARNING"] = RESULTS_FACETS_STRING
RESULTS_FACETS_ORDERING = {
    "table_id": ["Eday", "day", "Oday"],
    "grid_label": ["gn", "gr", "gr1", "gr2", "gr3", "gr4", "gr5", "gr6", "gr7", "gr8", "gr9"]
}
RELATIVE_PATH_FACETS = ["project", "activity_id", "source_id", "experiment_id", "member_id", 
                        "table_id", "variable", "version"]

# CMIP6
LOWEST_MODEL_RESOLUTIONS = { # lowest among ocean / land / atmoshpere (in km, as given on Uni Hamburg website)
 'ACCESS-CM2': 250,
 'AWI-ESM-1-1-LR': 250,
 'BCC-CSM2-MR': 100,
 'BCC-ESM1': 250,
 'CESM2': 100,
 'CMCC-CM2-HR4': 100,
 'CMCC-CM2-SR5': 100,
 'CMCC-ESM2': 100,
 'CNRM-CM6-1': 250,
 'CNRM-CM6-1-HR': 100,
 'CNRM-ESM2-1': 250,
 'EC-Earth3': 100,
 'EC-Earth3-CC': 100,
 'EC-Earth3-Veg': 100,
 'EC-Earth3-Veg-LR': 250,
 'GFDL-CM4': 100,
 'HadGEM3-GC31-LL': 250,
 'HadGEM3-GC31-MM':100,
 'IPSL-CM6A-LR': 250,
 'MIROC6': 250,
 'MPI-ESM-1-2-HAM': 250,
 'MPI-ESM1-2-HR': 100,
 'MPI-ESM1-2-LR': 200,
 'MRI-ESM2-0': 100,
 'UKESM1-0-LL': 250,
 'ACCESS-ESM1-5': 250,
 'BCC-CSM2-MR': 100
}
HR_MODELS = [k for k,v in LOWEST_MODEL_RESOLUTIONS.items() if v<=100]
LR_MODELS = [k for k,v in LOWEST_MODEL_RESOLUTIONS.items() if v>100]