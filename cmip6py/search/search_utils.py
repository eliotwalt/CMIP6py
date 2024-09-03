import pyesgf.search 
import requests.exceptions
import logging
from functools import lru_cache
import concurrent.futures

from ..commons.constants import PYESGF_CONFIG
from ..commons.utils import is_iterable_but_not_string, dict_product

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
        connection = pyesgf.search.SearchConnection(url=url, **pyesgf_config)
        context = connection.new_context(
            pyesgf.search.context.FileSearchContext,
            **facets,
        )
        logger.debug(f"Searching {url} for datasets using facets={facets}")
        try:
            url_results = list(context.search(
                batch_size=500,
                ignore_facet_check=True,
            ))
            logger.debug(f"Got {len(url_results)} results from {url} using facets={facets}")
            results.extend(url_results)
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError,
            requests.exceptions.Timeout
        ) as error:
            logger.debug(f"Unable to connect to {url} due to {error}")
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
    # convert facets to pyesgf facets format
    def convert_facet_values(values):
        # comma-separated iterables
        if is_iterable_but_not_string(values):
            return ",".join([convert_facet_values(v) for v in values])
        else:
            return str(values)
    # sequential search
    if max_workers == 1:
        facets = {facet: convert_facet_values(values) for facet, values in facets.items()}
        results = _search_esgf_nodes(**facets)
    # parallel search
    else:
        # split base and varying facets
        base_facets, varying_facets = {}, {}
        for facet, facet_val in facets.items():
            if facet=="table_id" or isinstance(facet_val, (str, int, float, bool)) or facet_val is None: base_facets[facet] = facet_val
            elif isinstance(facet_val, (list, tuple)): varying_facets[facet] = tuple(facet_val)
            else: raise TypeError(f"{facet} must be of type: list, tuple, str, int, float or bool. got {type(facet_val)}")
        # iterate over product
        individual_facets_list = dict_product(varying_facets)
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for individual_facets in individual_facets_list:
                # build these search facets
                these_facets = base_facets.copy()
                these_facets.update(individual_facets)
                # convert facets
                these_facets = {facet: convert_facet_values(values) for facet, values in these_facets.items()}
                # submit
                futures.append(executor.submit(
                    _search_esgf_nodes, **these_facets
                ))
            for future in concurrent.futures.as_completed(futures):
                these_results = future.result()
                results.extend(these_results)
    return results