from cmip6py.search.cmip6_search import search

search_facets = {"experiment_id": ["historical", "ssp245"], 
                 "source_id": ["EC-Earth3", "MPI-ESM1-2-HR"], 
                 "variable": ["ua", "va"], 
                 "table_id": ["Eday", "day", "Oday"]}
cmip6_search = search(
    search_facets,
    42,
    2,
    ["ua", "va"],
    save_path="test.pkl"
)