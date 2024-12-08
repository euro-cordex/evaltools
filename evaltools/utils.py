from collections import defaultdict


def iid_to_dict(dset_id, attrs):
    """
    Convert a dataset ID and its attributes to a dictionary.

    Parameters:
    dset_id (str): The dataset ID.
    attrs (dict): The dataset attributes.

    Returns:
    dict: The dataset ID and attributes as a dictionary.
    """
    values = dset_id.split(".")
    return dict(zip(attrs, values))


def dict_to_iid(attrs, drop=None):
    """
    Convert a dictionary of dataset attributes to a dataset ID.

    Parameters:
    attrs (dict): The dataset attributes.

    Returns:
    str: The dataset ID.
    """
    if drop is None:
        drop = []
    return ".".join(v for k, v in attrs.items() if k not in drop)


def sort_by_grid_mapping(dsets):
    """
    Sort the datasets by their grid mapping.

    Parameters:
    dsets (dict): A dictionary of xarray datasets.

    Returns:
    dict: A dictionary of xarray datasets sorted by grid mapping.
    """
    sorted = defaultdict(dict)
    for iid, ds in dsets.items():
        grid_mapping_name = ds.cf["grid_mapping"].grid_mapping_name
        sorted[grid_mapping_name][iid] = ds
    return sorted
