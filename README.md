# CMIP6py: a python package for CMIP6 data

CMIP6py allows to interact with CMIP6 data stored on ESGF nodes with python. More specifically, it allows:
1. Open search of CMIP6 data on ESGF nodes
2. Filtering and visualising CMIP6 data from ESGF nodes
3. Download CMIP6 data from ESGF nodes

The ESGF network can be somewhat unstable with nodes going offline and data becoming unreachable. CMIP6Py alleviates that issue by allowing to filter, almost in real-time, the running ESGF nodes. This allows to easily locate data on the network and donwload it safely without worrying about network status or duplicated data.

## Idea

The package is centered around three nested classes:
- `cmip6py.data.dataset.CMIP6Dataset`: Stores an entire dataset, i.e. model/experiment/member/variable for a given time period. Internally, it maintains a list of `cmip6.data.file.CMIP6File`s.
- `cmip6.data.CMIP6File`: Stores a single file within a dataset, i.e. model/experiment/member/variable spanning a time period contained in a larger dataset. Internally, it maintains a list of equivalent `cmip6py.data.entry.CMIP6Entry`s.
- `cmip6py.data.entry.CMIP6Entry`: Stores  a single instance of a given file that exist on an ESGF node. Entries are considered equivalent when they represent the same data with possible differences in the gridding (i.e. `table_id` and `grid_label` ESGF facets) and version (we might as well download a slighlty older version than nothing).

![class organisation overview](./imgs/classes_org.png "Organisation of CMIP6py main classes")

The entry point class is `CMIP6py.search.cmip6_search.CMIP6Search`, which allows to search ESGF network for any CMIP6 data. It leverages `CMIP6Dataset`s internally and provides additional functionalities such as filtering, balancing, visualisation and download. 

## Examples

Below is a basic CMIP6py workflow:
```
>>> from cmip6py import search

# defined search facets
>>> search_facets = {"experiment_id": ["historical", "ssp245", "ssp370"], "source_id": ["EC-Earth3", "MPI-ESM1-2-HR"], "variable": ["tas", "tos", "zg"], "table_id": ["Eday", "day", "Oday"]}

# create a CMIP6Search object and perform search
>>> cmip6_search = search(search_facets, random_seed=42, max_workers=6)
>>> cmip6_search
CMIP6Search:...

# explore nested structure
>>> cmip6_search.datasets[0]
CMIP6Dataset:...
>>> cmip6_search.datasets[0].files[0]
CMIP6File:...
>>> cmip6_search.datasets[0].files[0].entries[0]
CMIP6Entry:...

# plot members distribution
>>> cmip6_search.summary_plot()

# apply filtering
>>> filtered_cmip6_search = (cmip6search.filter("facets", experiment_id=["historical", "ssp245"]) # only historical and SSP-2.45
                                        .filter("years", historical=[2010, 2015], projections=[2015, 2021])) # select years
# balance members 
>>> balanced_cmip6_search = filtered_cmip6_search.balance_members(num_members=4, tolerance=2)
>>> cmip6_search.summary_plot()

# filter running nodes and download
>>> avail_cmip6_search = balanced_cmip6_search.filter("running_nodes")
>>> avail_cmip6_search.download("cmip6_data/")
>>> avail_cmip6_search.dataset_to_local_files
{
    CMIP6Dataset:... : [...],
}
```

`CMIP6Search` objects can also be splitted by dataset facets to be processed in parallel. This is particularly useful to build parallel data processing pipelines once the datasets have been downloaded.

```
>>> avail_cmip6_search.splitby(["source_id", "experiment_id"])
[CMIP6Search:...,
CMIP6Search:...,
...]
```

## Installation

Clone this repository and run
```
pip install -e .
```

## Troubleshooting

**Playwright Sync / Async API in Jupyter notebooks**

This error shows up when running `cmip6py.esgf_network.analytics.get_esgf_nodes_status()` from a jupyter notebook. Run the command in the termninal first to load the status file in cache. You can use the `init_esgf_nodes.py` script for that.
```
Error: It looks like you are using Playwright Sync API inside the asyncio loop.
Please use the Async API instead.
```




<!-- **`cmip6py.data.file.CMIP6File`**

This class ensures that we do not lose any data. A `CMIP6File` contains all equivalent CMIP6 files on ESGF, i.e. files that have the same `source_id`, `experiment_id`, `member_id`, `variable`, `start_date`, and `end_date`. Other facets may differ. The equivalent files informations are stored in a dataframe.
```
cmip6_file.name 
>>> {source_id}_{experiment_id}_{member_id}_{variable}_{start_date}-{end_date}

cmip6_file.df
>>> pd.DataFrame({
    source_id: ...,
    experiment_id: ...,
    member_id: ...,
    variable: ...,
    start_date: ...,
    end_date: ...,
    varying_facet-1: ...,
    varying_facet-2: ...,
    ...,
    varying_facet-N: ...,
    urls: ..., # all download urls associated to each file -> used for downloading
    data_nodes: ..., # all hosts associated to each file -> used for filtering runnning nodes
    checksums: ..., # all checksums associated to each file -> used to validate downloads
})
```

**`cmip6py.data.file.CMIP6Dataset`**

This class contains all `CMIP6File`s that have the same `source_id`, `experiment_id`, `member_id`, `variable` but different `start_date`, `end_date`.
```
cmip6_file.name 
>>> {source_id}_{experiment_id}_{member_id}_{variable}_{dataset_start_date}-{dataset_end_date}

cmip6_file.df
>>> pd.DataFrame({
    source_id: ...,
    experiment_id: ...,
    member_id: ...,
    variable: ...,
    dataset_start_date: ...,
    dataset_end_date: ...,
    varying_facet-1: ...,
    varying_facet-2: ...,
    ...,
    varying_facet-N: ...,
    urls: ..., # all download urls associated to each file -> used for downloading
    data_nodes: ..., # all hosts associated to each file -> used for filtering runnning nodes
    checksums: ..., # all checksums associated to each file -> used to validate downloads
})
```

## Functionalities

The main entrypoint is `cmip6py.open_search.CMIP6OpenSearch`. It allows to store search results, filter them and download them easily.

### Exploring  -->

