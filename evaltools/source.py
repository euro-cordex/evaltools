import xarray as xr
import intake
import pandas as pd
from warnings import warn

from .utils import iid_to_dict, dict_to_iid
from .eval import mask_with_sftlf, add_bounds

xarray_open_kwargs = {"use_cftime": True, "decode_coords": "all", "chunks": None}


def open_catalog(url=None):
    """
    Open a data catalog from a given URL. If no URL is provided, use the default URL.

    Parameters:
    url (str, optional): The URL of the data catalog. Defaults to None.

    Returns:
    intake.catalog: The opened data catalog.
    """
    if url is None:
        url = "https://raw.githubusercontent.com/euro-cordex/joint-evaluation/refs/heads/main/CORDEX-CMIP6.json"
    return intake.open_esm_datastore(url)


def get_source_collection(
    variable_id,
    frequency,
    driving_source_id="ERA5",
    add_fx=None,
    catalog=None,
    **kwargs,
):
    """
    Search the catalog for datasets matching the specified variable_id, frequency, and driving_source_id.

    Parameters:
    variable_id (str): The variable ID to search for.
    frequency (str): The frequency to search for.
    driving_source_id (str, optional): The driving source ID to search for. Defaults to "ERA5".
    add_fx (bool, optional): Whether to add fixed variables. Defaults to False.
    catalog (intake.catalog, optional): The data catalog to search in. If None, open the default catalog.

    Returns:
    intake.catalog: The filtered data catalog.
    """
    if add_fx is None:
        add_fx = "areacella"
    if catalog is None:
        catalog = open_catalog()
    subset = catalog.search(
        variable_id=variable_id,
        frequency=frequency,
        driving_source_id=driving_source_id,
        require_all_on=["source_id"],
        **kwargs,
    )
    source_ids = list(subset.df.source_id.unique())
    print(f"Found: {source_ids} for variables: {variable_id}")
    if add_fx:
        if add_fx is True:
            fx = catalog.search(source_id=source_ids, frequency="fx", **kwargs)
        else:
            fx = catalog.search(
                source_id=source_ids, frequency="fx", variable_id=add_fx, **kwargs
            )
            if fx.df.empty:
                warn(f"static variables not found: {variable_id}")
        subset.esmcat._df = pd.concat([subset.df, fx.df])
    return subset


def open_and_sort(catalog, merge=None, concat=False, time_range="auto"):
    """
    Convert the catalog to a dictionary of xarray datasets, sort them by source_id, and optionally merge or concatenate the datasets.

    Parameters:
    catalog (intake.catalog): The data catalog to convert and sort.
    merge (bool, optional): Whether to merge the datasets for each source_id. Defaults to None.
    concat (bool, optional): Whether to concatenate the datasets along the source_id dimension. Defaults to False.
    time_range (slice or str, optional): The time range to subset the datasets. Defaults to "auto".

    Returns:
    dict or xarray.Dataset: A dictionary of sorted (and optionally merged) xarray datasets, or a concatenated xarray.Dataset.
    """
    id_attrs = catalog.esmcat.aggregation_control.groupby_attrs

    if time_range == "auto":
        time_range = time_range_default

    dsets = catalog.to_dataset_dict(xarray_open_kwargs=xarray_open_kwargs)
    print(f"Found {len(dsets)} datasets")

    if time_range is not None:
        for iid, ds in dsets.items():
            if "time" in ds.dims:
                dsets[iid] = ds.sel(time=time_range)
    if merge:
        sorted = {
            dict_to_iid(iid_to_dict(iid, id_attrs), drop=merge): []
            for iid in catalog.keys()
        }
        # Merge variable_ids
        for iid, ds in dsets.items():
            new_iid = dict_to_iid(iid_to_dict(iid, id_attrs), drop=merge)
            sorted[new_iid].append(ds)
        for iid, dss in sorted.items():
            print(f"merging: {iid}")
            sorted[iid] = xr.merge(dss, compat="override")
        dsets = sorted
    if concat is True:
        ids = list(dsets.keys())
        concat_dim = xr.DataArray(ids, dims="iid", name="iid")
        return xr.concat(
            list(sorted.values()),
            dim=concat_dim,
            compat="override",
            coords="minimal",
            join="override",
        )
    return dsets


def open_datasets(variables, frequency="mon", mask=True, add_missing_bounds=True):
    catalog = get_source_collection(variables, frequency, add_fx=["areacella", "sftlf"])
    dsets = open_and_sort(catalog, merge=True)
    if mask is True:
        for ds in dsets.values():
            mask_with_sftlf(ds)
    if add_missing_bounds is True:
        for dset_id, ds in dsets.items():
            dsets[dset_id] = add_bounds(ds)
    return dsets
