import pandas as pd
import logging
from collections import defaultdict
import matplotlib.pyplot as plt
import seaborn as sns

from .search_utils import search_esgf_nodes
from ..data.dataset import CMIP6Dataset
from ..commons.constants import HR_MODELS, LR_MODELS

logger = logging.getLogger(__name__)

class CMIP6Search:
    def __init__(self, random_seed, max_workers):
        self.random_seed = random_seed
        self.max_workers = max_workers
        # attributes that are set by other methods
        self.datasets = None
        self.datasets_to_local_files = None
        # state flags
        self.nodes_are_filtered = False
        self.members_are_balanced = False
    
    def _filter_running_nodes(self):
        """
        Returns a copy that only has data from nodes that are running
        """
        new_datasets, new_datasets_to_local_files = [], {}
        # filter all datasets
        for dataset in self.datasets:
            dataset = dataset._filter_running_nodes()
            if self.datasets_to_local_files is not None and dataset in dataset.name in self.datasets_to_local_files.keys():
                new_datasets_to_local_files[dataset.name] = self.datasets_to_local_files[dataset.name]
            new_datasets.append(dataset)
        # create new search object
        new = CMIP6Search(self.random_seed, self.max_workers)
        if len(new_datasets_to_local_files)==0:
            new_datasets_to_local_files = None
        new.datasets = new_datasets
        new.datasets_to_local_files = new_datasets_to_local_files
        new.nodes_are_filtered = True
        return new
    
    def search(self, facets):
        # do search
        results = search_esgf_nodes(facets, self.max_workers)
        self.datasets = CMIP6Dataset.from_results(results)
    
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
    
    def balance_members(self, num_members, tolerance=0):
        """
        Returns a copy with balanced number of members per dataset
        """
        if not self.nodes_are_filtered:
            logging.warning(f"balancing members on unfiltered nodes. This will likely select the wrong data!")
        # new = ...
        # new.members_are_balanced = True
        raise NotImplementedError()
    
    def filter(self, kind, **kwargs):
        """
        dates, running_nodes, facets, variable_set, ...
        """
        if kind=="running_nodes":
            return self._filter_running_nodes()
        raise NotImplementedError() 
    
    def splitby(self, split_keys):
        """
        returns multiple `CMIP6Search`es by splitting according to
        `split_keys`
        """
        """
        don't forget to distribute self.datasets and self.datasets_to_local_file if they are not None
        """
        raise NotImplementedError()
        
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