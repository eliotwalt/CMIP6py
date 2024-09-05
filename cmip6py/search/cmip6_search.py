import pandas as pd
import logging
from collections import defaultdict
import matplotlib.pyplot as plt
import seaborn as sns
import random
from itertools import groupby
import os
import pickle 
import warnings

from .search_utils import search_esgf_nodes
from ..data.dataset import CMIP6Dataset
from ..commons.constants import HR_MODELS, LR_MODELS
from ..commons.utils import set_random_seed, is_iterable_but_not_string

logger = logging.getLogger(__name__)

class CMIP6Search:
    def __init__(self, random_seed, max_workers):
        self.random_seed = random_seed
        self.max_workers = max_workers
        # attributes that are set by other methods
        self.datasets = []
        self.datasets_to_local_files = {}
        # state flags
        self.nodes_are_filtered = False
        self.members_are_balanced = False
        # random seed
        set_random_seed(self.random_seed)
        # ADD TO self.copy() IF NEW ATTRIBUTES ARE ADDED
        
    def __len__(self):
        return 0 if self.datasets is None else len(self.datasets)
        
    def __repr__(self):
        return f"{self.__class__.__name__}:random_seed={self.random_seed},n_datasets={len(self)},nodes_are_filtered={self.nodes_are_filtered},members_are_balanced={self.members_are_balanced}"
    
    def save(self, path):
        """save to disk"""
        with open(path, mode="wb") as h:
            pickle.dump(self, h)
        return path

    @classmethod
    def load(cls, path):
        """load from disk"""
        with open(path, mode="rb") as h:
            search = pickle.load(h)
        return search.copy()
    
    def copy(self):
        new = CMIP6Search(self.random_seed, self.max_workers)
        new.datasets = self.datasets
        new.datasets_to_local_files = self.datasets_to_local_files
        new.nodes_are_filtered = self.nodes_are_filtered
        new.members_are_balanced = self.members_are_balanced
        return new
    
    def sub_copy(self, sub_datasets):
        new = CMIP6Search(self.random_seed, self.max_workers)
        new.datasets = sub_datasets
        new.datasets_to_local_files = {
            dataset.name: local_files for dataset, local_files in self.datasets_to_local_files.items()
            if dataset.name in [sds.name for sds in sub_datasets]
        }
        new.members_are_balanced = self.members_are_balanced
        new.nodes_are_filtered = self.nodes_are_filtered
        return new
    
    def strict_variable_set(self, variable_set):
        """
        Select model configurations (source_id, experiment_id, member_id) that have all the variables
        required in the variable set. Additionally, drops the datasets that concern other variables.
        """
        variable_set = set(variable_set)
        def same_model_configuration(dataset):
            return (dataset.source_id, dataset.experiment_id, dataset.member_id)
        valid_datasets = []
        datasets = sorted(self.datasets, key=same_model_configuration)
        # inspect all configurations
        for model_configuration, grouped_datasets in groupby(datasets, key=same_model_configuration):
            grouped_datasets = list(grouped_datasets)
            observed_variable_set = set(ds.variable for ds in grouped_datasets)
            # check if asked variable set is subset of obeserved one
            if variable_set.issubset(observed_variable_set):
                logger.info(f"{model_configuration} is valid {variable_set} is subset of {observed_variable_set}")
                valid_datasets.extend(grouped_datasets)
            else:
                logger.warning(f"{model_configuration} is NOT valid {variable_set} is NOT subset of {observed_variable_set}")
        # create new CMIP6Search from valid_datasets
        new = self.sub_copy(valid_datasets)
        # drop datasets that concern other variables
        new = new.filter(kind="facets", variable=list(variable_set))
        return new
    
    def count_members(self, as_pandas=False):
        members_count = defaultdict(int)
        for dataset in self.datasets:
            experiment_id, source_id = dataset.experiment_id, dataset.source_id
            members_count[(experiment_id, source_id)] += 1
        if as_pandas:
            data_list = [(k[1], k[0], v) for k, v in members_count.items()]
            # Create DataFrame from the list
            members_count = pd.DataFrame(data_list, columns=['source_id', 'experiment_id', 'num_members'])
        return members_count
    
    def balance_members(self, num_members, tolerance=0, filter_running_nodes=True):
        """
        Returns a copy with balanced number of members per dataset
        """
        if not self.nodes_are_filtered and not filter_running_nodes:
            logging.warning(f"balancing members on unfiltered nodes. This will likely select unreachable data!")
        if not self.nodes_are_filtered and filter_running_nodes:
            filtered_search = self._filter_running_nodes()
            # apply balance in the filtered search
            return filtered_search.balance_members(num_members, tolerance, False)
        # balancing logic
        # compute N members per source / exp
        min_members = num_members - tolerance # 2
        max_members = num_members + tolerance # 4
        balanced_mc = self.count_members(as_pandas=True).query(f"num_members >= {min_members}")
        balanced_mc.loc[:,"num_members"] = balanced_mc["num_members"].apply(lambda nm: min(nm, max_members))
        # select datasets randomly
        balanced_datasets = []
        def same_source_and_experiment(dataset):
            return (dataset.source_id, dataset.experiment_id)
        sorted_datasets = sorted(self.datasets.copy(), key=same_source_and_experiment)
        for source_exp, grouped_datasets in groupby(sorted_datasets, key=same_source_and_experiment):
            grouped_datasets = list(grouped_datasets)
            # extract N members for this source / experiment
            n_to_sample = balanced_mc.query(f"source_id == '{source_exp[0]}' & experiment_id == '{source_exp[1]}'")["num_members"].iloc[0]
            # shuffle and select 
            random.shuffle(grouped_datasets)
            balanced_datasets.extend(grouped_datasets[:n_to_sample])
        # create new object
        new = self.sub_copy(balanced_datasets)
        new.members_are_balanced = True
        return new
        
    def _filter_running_nodes(self):
        """
        Returns a copy that only has data from nodes that are running
        """
        if self.nodes_are_filtered:
            logging.info(f"Nodes are already balanced, not balancing them again.")
            return self.copy()
        new_datasets = []
        # filter all datasets
        for dataset in self.datasets:
            new_dataset = dataset._filter_running_nodes()
            # ensure non-empty dataset
            if new_dataset is not None: new_datasets.append(new_dataset)
        # create new search object
        new = self.sub_copy(new_datasets)
        # set flag
        new.nodes_are_filtered = True
        return new
    
    def _filter_facets(self, **facet_kwargs):
        """
        Filter datasets according to a set of facets. Returns a new `CMIP6Search`
        
        facet_kwargs (dict): `{facet_name: facet_values}`, with `facet_values` a non-string
        iterable
        """
        filtered_datasets = []
        # check each dataset to see if they match the filtering criteria
        for dataset in self.datasets:
            match = True
            for facet_name, facet_values in facet_kwargs.items():
                assert is_iterable_but_not_string(facet_values), f"{facet_name} must be an iterable, got values={facet_values}"
                # break if not a match
                if not getattr(dataset, facet_name) in facet_values:
                    match = False
                    break
            # add dataset if match was never false
            if match:
                filtered_datasets.append(dataset.copy())
        # create new cmip6_search
        new = self.sub_copy(filtered_datasets)
        return new
    
    def _filter_years(self, **span_kwargs):
        """
        Filter datasets to contain only files within the desired span. Returns a new `CMIP6Search`
        
        span_kwargs (dict): {"historical": [start, stop], "projections": [start, stop]}. Note
            that the start is included and stop is excluded. i.e. to get the full historical run you would
            need `"historical": [1850, 2015]"` and full projection run `"projections": [2015, 2101]"`
        """
        new_datasets = []
        # filter all datasets
        for dataset in self.datasets:
            # extract temporal span for the dataset's experiment
            experiment_temporal_span = span_kwargs["historical"] \
                                       if dataset.experiment_id=="historical" else \
                                       span_kwargs["projections"]
            new_dataset = dataset._filter_years(experiment_temporal_span)
            # ensure non-empty dataset
            if new_dataset is not None: new_datasets.append(new_dataset)
        # create new search object
        new = self.sub_copy(new_datasets)
        return new
    
    def filter(self, kind, **kwargs):
        """
        dates, running_nodes, facets, variable_set, ...
        """
        if kind=="running_nodes":
            return self._filter_running_nodes()
        if kind=="facets":
            return self._filter_facets(**kwargs)
        if kind=="years":
            return self._filter_years(**kwargs)
        raise NotImplementedError(f"filtering {kind} not implemented")
        
    def splitby(self, split_keys):
        """
        returns multiple `CMIP6Search`es by splitting according to `split_keys`
        
        Args:
        -----
            split_keys (list): list of ESGF facets, must all be in ["source_id", "experiment_id", "member_id", "variable"]
            
        Returns:
        --------

        """
        """
        don't forget to distribute self.datasets and self.datasets_to_local_file if they are not None
        """
        assert all(sk in ["source_id", "experiment_id", "member_id", "variable"] for sk in split_keys)
        def same_split(dataset):
            return tuple(getattr(dataset, sk) for sk in split_keys)
        new_searches = []
        datasets = sorted(self.datasets.copy(), key=same_split)
        for _, split_datasets in groupby(datasets, key=same_split):
            split_datasets = list(split_datasets)
            new_search = self.sub_copy(split_datasets)
            new_searches.append(new_search)
        return new_searches
    
    def search(self, facets):
        # do search
        results = search_esgf_nodes(facets, self.max_workers)
        logger.info(f"Got {len(results)} from ESGF nodes with facets={facets}")
        self.datasets = CMIP6Dataset.from_results(results)
        
    def download(self, dest_folder, max_workers=1):
        # check
        if self.datasets is None:
            logger.error(f"No search has been performed yet, nothing to download!")
        else:
            if not self.nodes_are_filtered:
                logger.warning(f"Downloading without having filtered nodes. This will likely result in many failed downloads!")
            if not self.members_are_balanced:
                logger.warning(f"Downloading without having balanced members. This could take up a lot of space on disk.")
            # launch downloads
            self.datasets_to_local_files = {
                dataset.name: dataset.download(dest_folder, max_workers)
                for dataset in self.datasets
            }
            
    def summary_plot(self, save_path=None):
        members_count = self.count_members(as_pandas=True)
        all_models = members_count.source_id.unique()   
        hr_models = list(set(all_models).intersection(set(HR_MODELS)))
        lr_models = list(set(all_models).intersection(set(LR_MODELS)))
        fig, axs = plt.subplots(ncols=3, nrows=1, figsize=(10, 5))
        axs = axs.flat
        for i, (resolution_group, models_group) in enumerate([
            ("", all_models),
            ("HR models only", hr_models),
            ("LR models only", lr_models),
        ]):
            sub_stats = members_count[members_count.source_id.isin(models_group)].sort_values(by="num_members", ascending=False)
            experiments = members_count.experiment_id.unique()
            hue_order = ["historical"] if "historical" in experiments else []
            hue_order += list(sorted([exp for exp in experiments if exp.startswith("ssp")], key=lambda x: int(x.replace("ssp", ""))))
            sns.barplot(data=sub_stats, y="source_id", x="num_members", hue="experiment_id", ax=axs[i], hue_order=hue_order)
            axs[i].set_ylabel(None)
            axs[i].set_title(resolution_group)
            axs[i].set_xlabel("num members")
            if i!=0: 
                axs[i].legend().remove()
            else: 
                axs[i].legend(ncols=3, bbox_to_anchor=(0.5, 1.0), loc="lower center", fontsize=6)
        fig.suptitle(f"Summary plot (members_are_balanced: {self.members_are_balanced}, nodes_are_filtered: {self.nodes_are_filtered})")
        plt.tight_layout()
        if save_path: fig.savefig(save_path)
        plt.show()
        
def search(
    facets, 
    random_seed=42,
    max_workers=2*os.cpu_count(), 
    variable_set=None,
    num_members=None,
    num_members_tolerance=None,
    filter_running_nodes=False,
    save_path=None,
    **filters
):
    """
    Search ESGF nodes according to the given facets. Extract all available 
    CMIP6 data
    
    Args:
    -----
        facets (dict): Facets directory
        random_seed (int): Random seed
        max_workers (int): Maximum number of workers for parallel search operations
        variable_set (list, None): If not None will apply `strict_variable_set` based
            on the list of variables provided in this argument
        num_members (int, None): If not None, members will be balanced according to 
            this argument
        num_members_tolerance (int, None): If not None, members will be balanced according to 
            this argument and `num_members`
        filter_running_nodes (bool): Whether to apply running nodes filtering. Defaults to 
            `False`. It is strongly advised to be set to `True` if members balancing is 
            activated.
        save_path (str, None): If not None the returned `CMIP6Search` object is saved
            at that location
        filters (dict): Additional filters. Keys must be in ['years', 'facets'], 
            e.g. `{"facets": {"source_id": ["EC-Earth3]}], "years": {"historical": [1980, 2014]}}`
        
    Returns:
    --------
        cmip6_search (CMIP6Search): The corresponding CMIP6Search object
        
    Warnings:
    ---------
        UserWarning: Members balancing activated without filtering running nodes

    Raises:
    -------
        AssertionError: Bad arguments
    """
    # Check arguments
    assert isinstance(facets, dict)
    assert isinstance(random_seed, int)
    assert isinstance(max_workers, int)
    assert isinstance(variable_set, list) or variable_set is None
    if isinstance(variable_set, list): 
        assert all(isinstance(v, str) for v in variable_set)
    assert isinstance(num_members, int) or num_members is None
    assert isinstance(num_members_tolerance, int) or num_members_tolerance is None
    if num_members_tolerance is not None:
        assert num_members is not None, "Setting `num_members_tolerance` requires setting `num_members`"
    assert isinstance(filter_running_nodes, bool)
    if num_members is not None and not filter_running_nodes:
        warnings.warn("Members balancing is activated but running nodes will not be filtered. "\
                      "This could lead to selecting unreachable data!", category=warnings.UserWarnings)
    assert isinstance(save_path, str)
    for filter_kind in filters.keys():
        assert filter_kind in ["years", "facets"], f"Invalid filter kind: {filter_kind}"        
    # basic search
    cmip6_search = CMIP6Search(random_seed, max_workers)
    cmip6_search.search(facets)
    # strict variable set
    if variable_set is not None:
        logger.info(f"filtering variable set: {variable_set}")
        cmip6_search = cmip6_search.strict_variable_set(variable_set=variable_set)
    # additional filters
    for filter_kind, filter_values in filters.items():
        logger.info(f"filtering {filter_kind}: {filter_values}")
        cmip6_search = cmip6_search.filter(filter_kind, **filter_values)
    # running nodes
    if filter_running_nodes:
        logger.info("filtering running nodes")
        cmip6_search = cmip6_search.filter("running_nodes")
    # balance members
    if num_members is not None:
        if num_members_tolerance is None: num_members_tolerance = 0
        logger.info(f"balancing members: N={num_members}±{num_members_tolerance}")
        cmip6_search = cmip6_search.balance_members(num_members=num_members,
                                     tolerance=num_members_tolerance)
    # save
    if save_path is not None:
        logger.info(f"saving CMIP6Search object to {save_path}")
        try:
            cmip6_search.save(save_path)
        except Exception as e:
            logger.error(f"Could not save CMIP6Search object at {save_path}: {e}")
    return cmip6_search