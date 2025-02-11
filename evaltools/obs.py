import xarray as xr
import numpy as np

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


def eobs(variable, add_mask=False, to_cf=False):
    """open EOBS dataset from LEAP

    See also: https://catalog.leap.columbia.edu/feedstock/eobs-dataset

    """
    store = "https://ncsa.osn.xsede.org/Pangeo/pangeo-forge/pangeo-forge/EOBS-feedstock/eobs-tg-tn-tx-rr-hu-pp.zarr"

    ds = xr.open_dataset(store, engine="zarr", chunks={})

    mask_var = [key for key, value in eobs_mapping.items() if value == variable][0]

    if add_mask is True:
        # create a simple mask from missing values
        ds["mask"] = xr.where(~np.isnan(ds[mask_var].isel(time=0)), 1, 0)

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
