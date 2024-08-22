import os
import logging
import hashlib
import shutil
from humanfriendly import format_size, format_timespan
from pathlib import Path
from itertools import groupby
from datetime import datetime
from urllib.parse import urlparse
from tempfile import TemporaryDirectory
import requests
import xarray as xr
from copy import deepcopy

from ..commons.constants import RESULTS_FACETS_ORDERING, RELATIVE_PATH_FACETS, ESGF_DOWNLOAD_TIMEOUT
from ..commons.utils import extract_esgf_file_datetimes
from ..commons.exceptions import DownloadError
from ..commons.utils import is_iterable_but_not_string

logger = logging.getLogger(__name__)

def get_version(result): return result.json["dataset_id"].split("|")[0].rsplit(".")[-1]

class CMIP6Entry:
    """
    Wrapper around `pyesgf.search.results.FileResult` with download utilities
    """
    def __init__(self, result):
        self.name = Path(result.filename).stem
        self.facets = self.format_facets(result)
        self.data_node = self.facets["data_node"]
        self.checksum = (result.checksum_type, result.checksum)
        self.url = result.download_url
        self.size = result.size
        self.entry_key = (self.facets["table_id"], self.facets["version"], self.facets["grid_label"])
        
    def __repr__(self):
        return f"{self.__class__.__name__}:{self.name}|{self.data_node}"
    
    def format_facets(self, result):
        facets = {facet: value[0] if is_iterable_but_not_string(value) else value
                 for facet, value in result.json.items()}
        # update version
        facets["version"] = get_version(result)
        return facets

    def _get_relative_path(self):
        return Path(*[self.facets[facet] for facet in RELATIVE_PATH_FACETS]) / Path(self.name).with_suffix(".nc")

    def _local_file(self, dest_folder):
        local_file = Path(dest_folder) / self._get_relative_path()
        os.makedirs(os.path.dirname(local_file), exist_ok=True)
        return local_file

    def _tmp_file(self):
        temp_dir = TemporaryDirectory().name
        tmp_file = Path(temp_dir) / self._get_relative_path()
        tmp_file.parent.mkdir(parents=True, exist_ok=True)
        os.makedirs(os.path.dirname(tmp_file), exist_ok=True)
        return tmp_file

    def download(self, dest_folder):
        """Download file from a single url."""
        checksum_type, checksum = self.checksum
        if checksum_type is None:
            hasher = None
        else:
            hasher = hashlib.new(checksum_type)
        # create local and temporary file
        local_file = self._local_file(dest_folder)
        tmp_file = self._tmp_file()
        # check if the file already exists
        if os.path.exists(local_file):
            try:
                xr.open_dataset(local_file)
                logger.info(f"{local_file} already exists. Not downloading.")
                return local_file
            except OSError as e:
                logger.error(f"{local_file} already exists but is not a valid netCDF4 file: {e}")
                raise e
        # start download
        logger.info(f"Downloading {self.name} from {self.data_node} to {tmp_file}")
        start_time = datetime.now()
        response = requests.get(self.url, stream=True, timeout=ESGF_DOWNLOAD_TIMEOUT)
        response.raise_for_status()
        # write data and hash
        with tmp_file.open("wb") as file:
            # Specify chunk_size to avoid
            # https://github.com/psf/requests/issues/5536
            megabyte = 2**20
            for chunk in response.iter_content(chunk_size=megabyte):
                if hasher is not None:
                    hasher.update(chunk)
                file.write(chunk)
        duration = datetime.now() - start_time
        if hasher is None:
            logger.warning(
                "No checksum available, unable to check data"
                " integrity for %s, ", self.url)
        else:
            local_checksum = hasher.hexdigest()
            if local_checksum != checksum:
                raise DownloadError(
                    f"Wrong {checksum_type} checksum for file {tmp_file},"
                    f" downloaded from {self.url}: expected {checksum}, but got"
                    f" {local_checksum}. Try downloading the file again.")
        # copy to local file
        shutil.move(tmp_file, local_file)
        logger.info("Downloaded %s (%s) in %s (%s/s) from %s", local_file,
                    format_size(self.size),
                    format_timespan(duration.total_seconds()),
                    format_size(self.size / duration.total_seconds()),
                    urlparse(self.url).hostname)
        return local_file
        
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
        entries = [CMIP6Entry(result) for result in results]
        # remove duplicates (due to the fact that we search ALL ESGF nodes)
        def same_entry(entry):
            return (entry.name, entry.data_node)
        entries = sorted(entries, key=same_entry)
        cmip6_entries = []
        for _, same_entries in groupby(entries, key=same_entry):
            same_entries = list(same_entries)
            cmip6_entries.append(same_entries[0])
        return cmip6_entries

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
        creates multiple `CMIP6File`s containing equivalent `pyesgf.search.results.ResultSet
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
    
    def filter_entries(self, entry_key_values, sort=False):
        filtered_entries, filtered_entry_keys = [], []
        # filter the entries and entry_keys
        for entry, entry_key in zip(self.entries, self.entry_keys):
            if entry_key in entry_key_values:
                filtered_entries.append(entry)
                filtered_entry_keys.append(entry_key)
        # sort according to entry_key_values if necessary
        if sort:
            # create mapping between current entries and entry keys
            entry_map = {entry_key: entry for entry_key, entry in zip(filtered_entry_keys, filtered_entries)}
            # sort entry keys according to entry_key_values
            sorted_filtered_entry_keys = list(sorted(filtered_entry_keys, key=lambda key: entry_key_values.index(key)))
            # get back entries in same order
            sorted_filtered_entries = []
            for entry_key in sorted_filtered_entry_keys:
                sorted_filtered_entries.append(entry_map[entry_key])
            # copy to previous lists
            filtered_entries = sorted_filtered_entries
            filtered_entry_keys = sorted_filtered_entry_keys
        # create a copy
        new = self.copy()
        new.entries = filtered_entries
        new.entry_keys = filtered_entry_keys
        return new
    
    def sample_entry_key(self, entry_key):
        """
        Sample the entry matching the `entry_key`
        """
        idx = self.entry_keys.index(entry_key)
        return self.entries[idx]