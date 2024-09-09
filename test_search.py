import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

from cmip6py.search.cmip6_search import search
from cmip6py.commons.constants import PYESGF_CONFIG

# faster
PYESGF_CONFIG = PYESGF_CONFIG.update({
    "urls": PYESGF_CONFIG["urls"][:2]
})

search_facets = {"experiment_id": ["historical"], 
                 "source_id": ["EC-Earth3"], 
                 "variable": ["ua", "va"], 
                 "table_id": ["Eday", "day", "Oday"]}
cmip6_search = search(
    search_facets,
    42,
    2,
    ["ua", "va"],
    save_path="test.pkl"
)