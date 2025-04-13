import xarray as xr
import numpy as np
from os import path as op

eobs_mapping = {
    "tg": "tas",
    "tn": "tasmin",
    "tx": "tasmax",
    "rr": "pr",
    "hu": "huss",
    "pp": "ps",
}

era5_cf_mapping = {
    "t2m": "tas",
    "t2m_min": "tasmin",
    "t2m_max": "tasmax",
    "tp": "pr",
    "q2": "huss",
    "msl": "ps",
}


# def eobs(add_mask=False, to_cf=False):
#     """open EOBS dataset from LEAP

#     See also: https://catalog.leap.columbia.edu/feedstock/eobs-dataset

#     """
#     store = "https://ncsa.osn.xsede.org/Pangeo/pangeo-forge/pangeo-forge/EOBS-feedstock/eobs-tg-tn-tx-rr-hu-pp.zarr"

#     ds = xr.open_dataset(store, engine="zarr", chunks={})

#     mask_var = "tg"

#     if add_mask is True:
#         # create a simple mask from missing values
#         ds["mask"] = xr.where(~np.isnan(ds[mask_var].isel(time=0)), 1, 0)

#     if to_cf is True:
#         pass

#     return ds


def eobs(
    add_mask=False,
    variables=None,
    add_elev=True,
    to_cf=False,
    version="v31.0e",
    source="local",
):
    """open EOBS dataset from LEAP

    See also: https://catalog.leap.columbia.edu/feedstock/eobs-dataset

    """
    import fsspec

    if variables is None:
        variables = ["tg", "tn", "tx", "rr", "hu"]
    elif isinstance(variables, str):
        variables = [variables]

    roots = {
        "local": "/mnt/CORDEX_CMIP6_tmp/aux_data/eobs",
        "aws": "https://knmi-ecad-assets-prd.s3.amazonaws.com/ensembles/data/Grid_0.1deg_reg_ensemble",
    }

    root = roots[source] if source in roots else source
    url_pattern = op.join(root, "{variable}_ens_mean_0.1deg_reg_{version}.nc")
    elev_url = op.join(root, f"elev_ens_0.1deg_reg_{version}.nc")

    datasets = [
        xr.open_dataset(
            fsspec.open(url_pattern.format(variable=var, version=version)).open(),
            chunks="auto",
        )
        for var in variables
    ]
    if add_elev is True:
        datasets.append(xr.open_dataset(fsspec.open(elev_url).open()))
    # return datasets
    ds = xr.merge(datasets, join="override")

    if add_mask is True:
        mask_var = variables[0]
        # create a simple mask from missing values
        ds["mask"] = xr.where(~np.isnan(ds[mask_var].isel(time=-1)), 1, 0)

    if to_cf is True:
        pass

    return ds


def era5():
    ds = xr.open_zarr(
        # "gs://gcp-public-data-arco-era5/ar/1959-2022-full_37-1h-0p25deg-chunk-1.zarr-v2",
        "gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3",
        chunks=None,
        storage_options=dict(token="anon"),
    )
    ds = ds.sel(time=slice(ds.attrs["valid_time_start"], ds.attrs["valid_time_stop"]))
    ds["mask"] = ds.land_sea_mask
    return ds
