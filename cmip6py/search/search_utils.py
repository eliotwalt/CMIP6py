import pyesgf.search 
import requests.exceptions
import pandas as pd
import logging
from functools import lru_cache

from ..esgf_network.analytics import get_esgf_nodes_status
from ..commons.constants import PYESGF_CONFIG
from ..commons.utils import is_iterable_but_not_string

logger = logging.getLogger(__name__)

@lru_cache(maxsize=10000)
def _search_esgf_nodes(**facets):
    """
    Search ESGF nodes according to the given facets. Extract all available 
    CMIP6 data
    
    Args:
    -----
        facets (dict): Facets directory in pyesgf format, i.e.
            `{facet_name: comma-separated values}`
        
    Returns:
    --------
        results (list): A list of `pyesgf.search.results.FileResult`s.
            
    Raises:
    -------
        ESGFSearchError: if the function cannot search ESGF nodes
    """
    pyesgf_config = PYESGF_CONFIG.copy()
    urls = pyesgf_config.pop("urls")
    if "project" not in facets.keys():
        facets["project"]= "CMIP6"
    # search loop
    errors = [] # gather errors
    results = [] # gather results for all urls (unlike ESMValCore)
    for url in urls:
        logger.info(f"Searching {url} for datasets using facets={facets}")
        connection = pyesgf.search.SearchConnection(url=url, **pyesgf_config)
        context = connection.new_context(
            pyesgf.search.context.FileSearchContext,
            **facets,
        )
        logger.info(f"Searching {url} for datasets using facets={facets}")
        try:
            url_results = list(context.search(
                batch_size=500,
                ignore_facet_check=True,
            ))
            logger.info(f"Got {len(url_results)} results from {url} using facets={facets}")
            results.extend(url_results)
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError,
            requests.exceptions.Timeout
        ) as error:
            logger.error(f"Unable to connect to {url} due to {error}")
            errors.append(error)
    if len(results)==0:
        raise FileNotFoundError("Failed to search ESGF, unable to connect:\n" +
                                "\n".join(f"- {e}" for e in errors))
    return results

def search_esgf_nodes(facets, max_workers=1):
    """
    Search ESGF nodes according to the given facets. Extract all available 
    CMIP6 data
    
    Args:
    -----
        facets (dict): Facets directory {facet_name: facet_value(s)}
        max_workers (int): Number of parallel search to launch
        
    Returns:
    --------
        results (list): A list of `pyesgf.search.results.FileResult`s.
            
    Raises:
    -------
        ESGFSearchError: if the function cannot search ESGF nodes
    """
    if max_workers > 1:
        logger.warning(f"max_workers > 1 not implemented yet. searching sequentially")
    # convert facets to pyesgf facets format
    def convert_facet_values(values):
        # comma-separated iterables
        if is_iterable_but_not_string(values):
            return ",".join([convert_facet_values(v) for v in values])
        else:
            return str(values)
    facets = {facet: convert_facet_values(values) for facet, values in facets.items()}
    # search
    results = _search_esgf_nodes(**facets)
    return results