import os
import logging
import hashlib
import shutil
from humanfriendly import format_size, format_timespan
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse
from tempfile import TemporaryDirectory
import requests
import xarray as xr

from ..esgf_network.analytics import get_esgf_nodes_status
from ..commons.constants import RELATIVE_PATH_FACETS, ESGF_DOWNLOAD_TIMEOUT
from ..commons.exceptions import DownloadError
from ..commons.utils import is_iterable_but_not_string
from .data_utils import get_version

logger = logging.getLogger(__name__)

class CMIP6Entry:
    """
    Wrapper around `pyesgf.search.results.FileResult` with download utilities
    """
    def __init__(self, result):
        self.result = result # for copying
        if "context" in self.result.__dict__.keys():
            # context is a CachedSession object which cannot be pickled
            self.result.__dict__.pop("context")
        self.facets = self.format_facets(result)
        self.data_node = self.facets["data_node"]
        self.checksum = (result.checksum_type, result.checksum)
        self.url = result.download_url
        self.size = result.size
        self.entry_key = (self.facets["table_id"], self.facets["version"], self.facets["grid_label"])
        self.name = f"{Path(result.filename).stem}|{self.data_node}" # for __repr__
        self.filename = Path(result.filename).stem # for filename when downloading
        
    def __repr__(self):
        return f"{self.__class__.__name__}:{self.name}"
    
    def copy(self):
        return CMIP6Entry(self.result)
    
    def format_facets(self, result):
        facets = {facet: value[0] if is_iterable_but_not_string(value) else value
                 for facet, value in result.json.items()}
        # update version
        facets["version"] = get_version(result)
        return facets
    
    def is_on_running_node(self):
        """
        Check whether the data_node is running
        """
        esgf_nodes_status = get_esgf_nodes_status()
        if self.data_node not in esgf_nodes_status.keys():
            logger.debug(f"{self} data_node {self.data_node} not part of the esgf_nodes_status dictionary! Will return False.")
        return esgf_nodes_status.get(self.data_node, False)

    def _get_relative_path(self):
        return Path(*[self.facets[facet] for facet in RELATIVE_PATH_FACETS]) / Path(self.filename).with_suffix(".nc")

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
                logger.warning(f"{local_file} already exists and is a valid netCDF4 file. Not downloading.")
                return local_file
            except OSError as e:
                logger.error(f"{local_file} already exists but is not a valid netCDF4 file: {e}")
                raise e
        # start download
        logger.debug(f"Downloading {self.name} from {self.data_node} to {tmp_file}")
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
                f" integrity for {self.url}, ")
        else:
            local_checksum = hasher.hexdigest()
            if local_checksum != checksum:
                raise DownloadError(
                    f"Wrong {checksum_type} checksum for file {tmp_file},"
                    f" downloaded from {self.url}: expected {checksum}, but got"
                    f" {local_checksum}. Try downloading the file again.")
        # copy to local file
        shutil.move(tmp_file, local_file)
        logger.debug("Downloaded {} ({}) in {} ({}{}) from {}".format(
                    local_file,
                    format_size(self.size),
                    format_timespan(duration.total_seconds()),
                    format_size(self.size / duration.total_seconds()),
                    urlparse(self.url).hostname))
        return local_file