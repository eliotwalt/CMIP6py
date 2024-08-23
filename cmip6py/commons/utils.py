import shutil
import os
from pathlib import Path
from datetime import datetime
from collections.abc import Iterable
import random 
import numpy as np

from .constants import CACHE_DIR

def clear_cache():
    shutil.rmtree(CACHE_DIR)
    os.makedirs(CACHE_DIR, exists_ok=True)
    
def is_iterable_but_not_string(x):
    return isinstance(x, Iterable) and not isinstance(x, (str, bytes, bytearray))
    
def remove_all_extensions(file_path: str) -> str:
    p = Path(file_path)
    while p.suffix:
        p = p.with_suffix('')
    return str(p)
    
def extract_esgf_file_datetimes(file_name, as_datetime=False):
    """
    Extract dates as python datetime objects from filenames with pattern
    'tos_Oday_AWI-CM-1-1-MR_historical_r1i1p1f1_gn_18500101-18501231.nc'
    and extract the number of years between start and stop dates.
    """
    start, stop = remove_all_extensions(file_name).split("_")[-1].split("-")
    if as_datetime:
        start = datetime.strptime(start, "%Y%m%d")
        stop = datetime.strptime(stop, "%Y%m%d")
    return start, stop

def convert_esgf_file_datetime(dt):
    if isinstance(dt, datetime):
        return dt.strftime("%Y%m%d")
    elif isinstance(dt, str):
        return datetime.strptime(dt, "%Y%m%d")
    
def convert_version_to_datetime(version):
    return datetime.strptime(version, "v%Y%m%d")

def set_random_seed(seed):
    random.seed(seed)
    np.random.seed(seed)
    
def overlapping_spans(file_start_year, file_stop_year, exp_start_year, exp_stop_year):
    """
    Checks that [file_start_year, file_stop_year] overlaps with [exp_start_year, exp_stop_year],
    where file_stop_year is included if it matches the bound, but file_start_year is excluded if it matches the bound.
    """
    start_condition = file_start_year < exp_stop_year
    end_condition = file_stop_year >= exp_start_year
    return start_condition and end_condition
