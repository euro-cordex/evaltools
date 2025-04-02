import xarray as xr
import intake
import pandas as pd
import warnings

from .utils import iid_to_dict, dict_to_iid
from .eval import mask_with_sftlf, add_bounds
from .fix import check_and_fix, FixException

# we use cftime for time handling
# do not decode coords by default since open_mfdataset might lose encoding
# see, e.g., https://github.com/pydata/xarray/issues/2436#issuecomment-449737841
xarray_open_kwargs = {"use_cftime": True, "decode_coords": None, "chunks": {}}

# default time range for the evaluation
time_range_default = slice("1979", "2020")

xr.set_options(keep_attrs=True)


def open_catalog(url=None):
    """
    Open a data catalog from a given URL. If no URL is provided, use the default URL.

    Parameters
    ----------
    url : str, optional
        The URL of the data catalog. Defaults to None.

    Returns
    -------
    intake.catalog
        The opened data catalog.
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

    Parameters
    ----------
    variable_id : str
        The variable ID to search for.
    frequency : str
        The frequency to search for.
    driving_source_id : str, optional
        The driving source ID to search for. Defaults to "ERA5".
    add_fx : bool or list of str, optional
        Whether to add fixed variables (e.g., "areacella", "sftlf"). Defaults to None.
    catalog : intake.catalog, optional
        The data catalog to search in. If None, the default catalog is opened.
    **kwargs : dict
        Additional arguments passed to the catalog search.

    Returns
    -------
    intake.catalog
        The filtered data catalog.
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
                warnings.warn(f"static variables not found: {variable_id}")
        subset.esmcat._df = pd.concat([subset.df, fx.df])
    return subset


def open_and_sort(
    catalog, merge_fx=False, concat=False, time_range="auto", apply_fixes=False
):
    """
    Convert the catalog to a dictionary of xarray datasets, sort them by source_id, and optionally merge or concatenate the datasets.

    Parameters
    ----------
    catalog : intake.catalog
        The data catalog to convert and sort.
    merge_fx : bool, optional
        Whether to merge the datasets for each source_id. Defaults to False.
    concat : bool, optional
        Whether to concatenate the datasets along the source_id dimension. Defaults to False.
    time_range : slice or str, optional
        The time range to subset the datasets. Defaults to "auto".
    apply_fixes : bool, optional
        Whether to apply fixes to the datasets. Defaults to False.

    Returns
    -------
    dict or xarray.Dataset
        A dictionary of sorted (and optionally merged) xarray datasets, or a concatenated xarray.Dataset.
    """
    if concat is True and not merge_fx:
        merge_fx = True

    if time_range == "auto":
        time_range = time_range_default

    dsets = catalog.to_dataset_dict(xarray_open_kwargs=xarray_open_kwargs)

    for iid, ds in dsets.items():
        print("decoding dataset", iid)
        # Check for warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")  # Catch all warnings
            dsets[iid] = xr.decode_cf(ds, decode_coords="all")  # Call the function

            # Check if any warnings were raised
            if w:
                for warning in w:
                    print(f"Warning for {iid}: {warning.message}")

            print(f"Found {len(dsets)} datasets")

    fixed_dsets = {}

    for iid, ds in dsets.items():
        if "time" in ds.dims:
            fixed_dsets[iid] = ds.sel(time=time_range)
        if apply_fixes:
            try:
                fixed_dsets[iid] = check_and_fix(ds, iid)
            except FixException as e:
                print(f"Fix failed for {iid}: {e}")
                print(f"Dataset {iid} will be ignored...")
                continue

    dsets = fixed_dsets

    if merge_fx is True:
        id_attrs = catalog.esmcat.aggregation_control.groupby_attrs
        # Merge fx datasets
        freq = "frequency"
        dsets_merged = {}
        # Merge
        for iid, ds in dsets.items():
            attrs = iid_to_dict(iid, id_attrs)
            if attrs[freq] != "fx":
                fx_iid = dict_to_iid(attrs | {"frequency": "fx"})
                if fx_iid in dsets:
                    print(f"merging {iid} with {fx_iid}")
                    dsets_merged[iid] = xr.merge(
                        [ds, dsets[fx_iid]],
                        compat="override",
                        join="override",
                        combine_attrs="override",
                    )
        dsets = dsets_merged
    if concat is True:
        ids = list(dsets.keys())
        concat_dim = xr.DataArray(ids, dims="iid", name="iid")
        return xr.concat(
            list(dsets.values()),
            dim=concat_dim,
            compat="override",
            coords="minimal",
            join="override",
        )
    return dsets


def open_datasets(variables, frequency="mon", mask=True, add_missing_bounds=True):
    """
    Open datasets for the specified variables and frequency, optionally applying masks and adding missing bounds.

    Parameters
    ----------
    variables : list of str
        The variables to open.
    frequency : str, optional
        The frequency of the datasets. Defaults to "mon".
    mask : bool, optional
        Whether to apply masks using "sftlf". Defaults to True.
    add_missing_bounds : bool, optional
        Whether to add missing bounds to the datasets. Defaults to True.

    Returns
    -------
    dict
        A dictionary of xarray datasets.
    """
    catalog = get_source_collection(variables, frequency, add_fx=["areacella", "sftlf"])
    dsets = open_and_sort(catalog, merge_fx=True)
    if mask is True:
        for ds in dsets.values():
            mask_with_sftlf(ds)
    if add_missing_bounds is True:
        for dset_id, ds in dsets.items():
            dsets[dset_id] = add_bounds(ds)
    return dsets
