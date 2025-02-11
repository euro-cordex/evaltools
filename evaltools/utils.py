from collections import defaultdict

def iid_to_dict(iid, attrs=None):
    """
    Convert a dataset ID and its attributes to a dictionary.

    Parameters:
    dset_id (str): The dataset ID.
    attrs (dict): The dataset attributes.

    Returns:
    dict: The dataset ID and attributes as a dictionary.
    """
    values = iid.split(".")
    return dict(zip(attrs, values))

def dict_to_iid(attrs, drop=None, delimiter="."):
    """
    Convert a dictionary of dataset attributes to a dataset ID.

    Parameters:
    attrs (dict): The dataset attributes.

    Returns:
    str: The dataset ID.
    """
    if drop is None:
        drop = []
    return delimiter.join(v for k, v in attrs.items() if k not in drop)


def short_iid(iid, attrs=None, delimiter=".", default_attrs = None):
    """
    Convert a dataset ID to a short ID.

    Parameters:
    iid (str): The dataset ID.
    attrs (dict): The dataset attributes.

    Returns:
    str: The short ID.
    """
    if attrs is None:
        attrs = ["institution_id", "source_id", "driving_source_id", "experiment_id"]
    return dict_to_iid(
        {k: v for k, v in iid_to_dict(iid, default_attrs).items() if k in attrs}, delimiter=delimiter
    )

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
