def get_facets(result, as_list=False):
    if isinstance(result, dict):
        if as_list:
            return {k: [v] for k, v in result["facets"].items()}
        else:
            return result["facets"]
    else:
        return result.json

def get_version(result): 
    return get_facets(result)["dataset_id"].split("|")[0].rsplit(".")[-1]