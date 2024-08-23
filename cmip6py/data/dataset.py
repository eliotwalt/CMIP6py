import logging
from itertools import groupby
import concurrent.futures
from collections import defaultdict

from .file import CMIP6File
from ..commons.utils import convert_esgf_file_datetime, convert_version_to_datetime, overlapping_spans
from ..commons.exceptions import DownloadError
from ..commons.constants import RESULTS_FACETS_ORDERING

logger = logging.getLogger(__name__)

class CMIP6Dataset:
    def __init__(self, files):
        self.source_id = files[0].source_id
        self.experiment_id = files[0].experiment_id
        self.member_id = files[0].member_id
        self.variable = files[0].variable
        self.start_date = min(file.start_date for file in files)
        self.end_date = max(file.end_date for file in files)
        self.name = self._make_name()
        # files
        self.files = files
        self.entry_keys_set = self._intersect_entry_keys(files)

    def __len__(self):
        return len(self.files)

    def __getitem__(self, idx):
        return self.files[idx]

    def __repr__(self):
        return f"{self.__class__.__name__}:{self.name}"
    
    def copy(self):
        return CMIP6Dataset(self.files)

    def _make_name(self):
        return self.source_id + "_" + \
               self.experiment_id + "_" + \
               self.member_id + "_" + \
               self.variable + "_" + \
               convert_esgf_file_datetime(self.start_date) + "-" + \
               convert_esgf_file_datetime(self.end_date)

    def _intersect_entry_keys(self, files):
        """
        filter the `CMIP6File`s' `CMIP6Entry`'s such that they only contain `entry_key`'s that are 
            part of the intersection of all `entry_key`'s

        Returns
        -------
            files (list): list of CMIP6Files with all entries part of `entries_keys_set`
            entry_keys_set (set): common entries_keys across all files
        """
        # extract entry_key set
        entry_keys_set = set(files[0].entry_keys)
        for file in files:
            if len(entry_keys_set)==0: entry_keys_set = set(file.entry_keys)
            else: entry_keys_set &= set(file.entry_keys)   
        # sort entry_key_set
        versions = sorted([convert_version_to_datetime(entry_key[1]) for entry_key in entry_keys_set])
        def get_sort_key(entry_key):
            return (
                RESULTS_FACETS_ORDERING["table_id"].index(entry_key[0]),
                versions.index(convert_version_to_datetime(entry_key[1])),
                RESULTS_FACETS_ORDERING["grid_label"].index(entry_key[2]),
            )
        entry_keys_set = list(sorted(entry_keys_set, key=get_sort_key))
        return entry_keys_set
    
    @staticmethod
    def from_results(results):
        """
        creates multiple `CMIP6Datasets`s with matching `pyesgf.search.results.ResultSet`
        """
        files = CMIP6File.from_results(results)
        return CMIP6Dataset.from_cmip6_files(files)

    @classmethod
    def from_cmip6_files(cls, files):
        """
        creates multiple `CMIP6Datasets`s with matching `CMIP6File`s
        """
        def matching_dataset(file):
            return  "_".join(file.name.split("_")[:-1])
        cmip6_datasets = []
        files = sorted(files, key=matching_dataset)
        for matching_name, matching_files in groupby(files, key=matching_dataset):
            matching_files = list(matching_files)
            logger.debug(f"Creating CMIP6Dataset from {matching_name} ({len(matching_files)} files)")
            cmip6_dataset = cls(matching_files)
            cmip6_datasets.append(cmip6_dataset)
        return cmip6_datasets
    
    def _filter_running_nodes(self):
        """
        returns a copy of the dataset in which the entries of all files exist on nodes that are currently running
        
        WARNING: make sure to check for empty datasets and filter them out!!!!
        """
        filtered_files = []
        # filter every file
        for file in self.files:
            filtered_file = file._filter_running_nodes()
            if filtered_file is not None:
                filtered_files.append(filtered_file)
        # return none if empty
        if len(filtered_files) == 0:
            logger.warning(f"Filtering runnning nodes on {self} resulted in empty dataset.")
            return None
        # create new
        new_dataset = CMIP6Dataset(filtered_files)
        return new_dataset
    
    def _filter_years(self, temporal_span):
        """
        Filters files that are in the temporal_span. Returns a new `CMIP6Dataset` instance.
        """
        start_year, stop_year = temporal_span
        filtered_files = []
        # check each file to see if they are in span
        for file in self.files:
            if overlapping_spans(file.start_date.year, file.end_date.year, start_year, stop_year):
                filtered_files.append(file.copy())
        if len(filtered_files)==0:
            logger.warning(f"Filtering years {start_year} to {stop_year} on {self} resulted in empty dataset.")
            return None
        # crearte new
        new_dataset = CMIP6Dataset(filtered_files)
        return new_dataset

    def sample_entry_key(self, entry_key):
        """
        sample an entry key by sampling the corresponding entry from each file
        """
        entries = [f.sample_entry_key(entry_key) for f in self.files]
        return entries
    
    def _donwload_entry(self, entry, dest_folder):
        try:
            local_file = entry.download(dest_folder)
            error = None
        except Exception as e:
            logger.debug(f"Failed to download {entry} due to: {e}")
            error = e
            local_file = None
        return local_file, error

    def download(self, dest_folder, max_workers=1):
        """
        Attempts to download a consistent dataset made of `CMIP6Entry`'s that share the same
        `entry_key`.

        Args:
        -----
            dest_folder (str): path to destination folder
            max_workers (int): number of workers for parallel download attempts

        Returns:
        --------
            local_files (list): list of local file paths containing the downloaded data

        Raises:
        -------
            DownlaodError: if no consistent dataset could be downloaded from any entry_key
        """
        errors = defaultdict(list)
        for entry_key in self.entry_keys_set:
            # get dataset entries
            entries = self.sample_entry_key(entry_key)
            logger.info(f"Attempting to download {self} with entries_key={entry_key} and {len(entries)} individual entries (max parallel jobs: {max_workers})")
            # download 
            local_files = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(self._donwload_entry, entry, dest_folder) for entry in entries]
                for future in concurrent.futures.as_completed(futures):
                    local_file, error = future.result()
                    if error is not None: errors[entry_key].append(error)
                    if local_file is not None: local_files.append(local_file)
            # check whether we succeeded
            if errors[entry_key] == []:
                logger.info(f"Successfully downloaded {self} with entries_key={entry_key}")
                return local_files
        # all download failed if we reach this point
        raise DownloadError(f"Failed to download {self} from any entry_key:\n"+
                            "\n".join(f"- {entry_key}: {error}" for entry_key, error in errors.items()))