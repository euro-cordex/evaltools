import xarray as xr
import intake

xarray_open_kwargs = {"use_cftime": True, "decode_coords": "all", "chunks": None}
time_range_default = slice("1979", "2020")


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


def get_source_collection(variable_id, frequency, fx=False, catalog=None):
    """
    Search the catalog for datasets matching the specified variable_id and frequency.

    Parameters:
    variable_id (str): The variable ID to search for.
    frequency (str): The frequency to search for.
    catalog (intake.catalog, optional): The data catalog to search in. If None, open the default catalog.

    Returns:
    intake.catalog: The filtered data catalog.
    """
    if catalog is None:
        catalog = open_catalog()
    cat = catalog.search(
        variable_id=variable_id, frequency=frequency, require_all_on=["source_id"]
    )
    print(f"Found: {list(cat.df.source_id.unique())} for variables: {variable_id}")
    return cat


def open_and_sort(catalog, merge=False, concat=False, time_range="auto"):
    """
    Convert the catalog to a dictionary of xarray datasets, sort them by source_id, and optionally merge the datasets.

    Parameters:
    catalog (intake.catalog): The data catalog to convert and sort.
    merge (bool): Whether to merge the datasets for each source_id. Defaults to False.

    Returns:
    dict: A dictionary of sorted (and optionally merged) xarray datasets.
    """
    if time_range == "auto":
        time_range = time_range_default
    if concat is True:
        merge = True
    source_ids = list(catalog.df.source_id.unique())
    dsets = catalog.to_dataset_dict(xarray_open_kwargs=xarray_open_kwargs)
    print(f"Found {len(dsets)} datasets")
    sorted = {}
    for source_id in source_ids:
        sorted[source_id] = [
            dsets[key] for key in dsets if dsets[key].attrs["source_id"] == source_id
        ]
    if time_range is not None:
        for source_id in source_ids:
            print(f"subsetting: {source_id}")
            for i, ds in enumerate(sorted[source_id]):
                if "time" in ds.dims:
                    sorted[source_id][i] = ds.sel(time=time_range)
    if merge is True:
        for source_id in source_ids:
            print(f"merging: {source_id}")
            sorted[source_id] = xr.merge(sorted[source_id])
    if concat is True:
        ids = list(sorted.keys())
        concat_dim = xr.DataArray(ids, dims="source_id", name="source_id")
        return xr.concat(
            list(sorted.values()),
            dim=concat_dim,
            compat="override",
            coords="minimal",
            join="override",
        )
    return sorted
