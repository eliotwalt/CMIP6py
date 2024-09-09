import logging
from itertools import groupby
from datetime import datetime
from copy import deepcopy

from ..commons.constants import RESULTS_FACETS_ORDERING
from ..commons.utils import extract_esgf_file_datetimes
from .entry import CMIP6Entry
from .data_utils import get_version

logger = logging.getLogger(__name__)
        
class CMIP6File:
    def __init__(self, results):
        # Initialise static facets from the first result in the list
        facets = results[0].json.copy()
        self.source_id = facets.pop("source_id")[0]
        self.experiment_id = facets.pop("experiment_id")[0]
        self.member_id = facets.pop("member_id")[0]
        self.variable = facets.pop("variable")[0]
        self.start_date, self.end_date = extract_esgf_file_datetimes(results[0].filename, as_datetime=True)
        self.name = CMIP6File._make_name(results[0])
        # Create varying facets
        self.entries = self._convert_to_sorted_entries(results)
        self.entry_keys = [entry.entry_key for entry in self.entries]
        
    def __len__(self):
        return len(self.entries)

    def __getitem__(self, idx):
        return self.entries[idx]

    def __repr__(self):
        return f"{self.__class__.__name__}:{self.name}"
    
    def copy(self): 
        return deepcopy(self)

    def _convert_to_sorted_entries(self, results):
        """
        sort the results according to:
            1. their table_id (see default ordering in cmip6py.commons.constants.RESULTS_FACETS_ORDERING)
            2. version (latest first)
            3. their grid_label (see default ordering in cmip6py.commons.constants.RESULTS_FACETS_ORDERING)
        and convert them into CMIP6Entries
        """
        # gather versions
        versions = []
        _results = []
        for result in results:
            version = get_version(result)
            try: _ = datetime.strptime(version, "v%Y%m%d")
            except Exception as e: logger.error(f"Ignoring invalid version: {version} in {self.name}: {e}")
            else:
                versions.append(version)
                _results.append(result)
        results = _results
        # sort versions (latest first)
        versions = list(sorted(versions, reverse=True))
        # sort results
        def get_sort_key(result):
            return (
                RESULTS_FACETS_ORDERING["table_id"].index(result.json["table_id"][0]),
                versions.index(get_version(result)),
                RESULTS_FACETS_ORDERING["grid_label"].index(result.json["grid_label"][0]),
            )                
        results = list(sorted(results, key=get_sort_key))
        # convert to enrties
        entries = [CMIP6Entry.from_result(result) for result in results]
        # remove duplicates (due to the fact that we search ALL ESGF nodes)
        unique_entries = CMIP6File._remove_duplicate_entries(entries)
        return unique_entries
    
    @staticmethod
    def _remove_duplicate_entries(entries):
        def same_entry(entry):
            return (entry.name)
        entries = sorted(entries, key=same_entry)
        unique_entries = []
        for same_key, same_entries in groupby(entries, key=same_entry):
            same_entries = list(same_entries)
            unique_entries.append(same_entries[0])
        return unique_entries

    @staticmethod
    def _make_name(result):
        facets = result.json
        start_date, end_date = extract_esgf_file_datetimes(result.filename, as_datetime=False)
        return facets["source_id"][0] + "_" + \
               facets["experiment_id"][0] + "_" + \
               facets["member_id"][0] + "_" + \
               facets["variable"][0] + "_" + \
               start_date  + "-" + \
               end_date

    @classmethod
    def from_results(cls, results):
        """
        creates multiple `CMIP6File`s containing equivalent `pyesgf.search.results.ResultSet`
        """
        def equivalent_file(result):
            return CMIP6File._make_name(result)
        cmip6_files = []
        results = sorted(results, key=equivalent_file)
        for _, equivalent_file_results in groupby(results, key=equivalent_file):
            equivalent_file_results = list(equivalent_file_results)
            cmip6_file = cls(equivalent_file_results)
            cmip6_files.append(cmip6_file)
        return cmip6_files
    
    def _filter_running_nodes(self):
        """
        Filter entries that are on running nodes and create a new file from their 
        pyesgf results
        """
        # filter entries and retrieve the "result" object for the CMIP6File constructor
        new_entry_results = []
        for entry in self.entries:
            if entry.is_on_running_node():
                new_entry_results.append(entry.result)
        # return none if empty
        if len(new_entry_results)==0:
            logger.warning(f"Filtering running nodes on {self} resulted in empty file!")
            return None
        # create new CMIP6File
        return CMIP6File(new_entry_results)        
    
    def sample_entry_key(self, entry_key):
        """
        Sample the entry matching the `entry_key`
        """
        idx = self.entry_keys.index(entry_key)
        return self.entries[idx]