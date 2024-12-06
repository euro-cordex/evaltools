import xarray as xr
import intake


xarray_open_kwargs = {"use_cftime": True, "decode_coords": "all"}


def open_catalog(url=None):
    if url is None:
        url = "https://raw.githubusercontent.com/euro-cordex/joint-evaluation/refs/heads/main/CORDEX-CMIP6.json"
    return intake.open_esm_datastore(url)


def get_source_collection(variable_id, frequency, catalog=None):
    if catalog is None:
        catalog = open_catalog()
    cat = catalog.search(
        variable_id=variable_id, frequency=frequency, require_all_on=["source_id"]
    )
    print(f"Found: {list(cat.df.source_id.unique())} for variables: {variable_id}")
    return cat


def open_and_sort(catalog, merge=False):
    source_ids = list(catalog.df.source_id.unique())
    dsets = catalog.to_dataset_dict(xarray_open_kwargs=xarray_open_kwargs)
    print(f"Found {len(dsets)} datasets")
    sorted = {}
    for source_id in source_ids:
        sorted[source_id] = [
            dsets[key] for key in dsets if dsets[key].attrs["source_id"] == source_id
        ]
    if merge is True:
        for source_id in source_ids:
            print(f"merging: {source_id}")
            sorted[source_id] = xr.merge(sorted[source_id])
    return sorted
