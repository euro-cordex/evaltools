import xarray as xr
import numpy as np


def eobs():
    """open EOBS dataset from LEAP
    
    See also: https://catalog.leap.columbia.edu/feedstock/eobs-dataset
    
    """
    store = "https://ncsa.osn.xsede.org/Pangeo/pangeo-forge/pangeo-forge/EOBS-feedstock/eobs-tg-tn-tx-rr-hu-pp.zarr"

    ds = xr.open_dataset(store, engine="zarr", chunks={})

    mask_var = "tg"

    if add_mask is True:
        # create a simple mask from missing values
        ds["mask"] = xr.where(~np.isnan(ds[mask_var].isel(time=0)), 1, 0)

    return ds


def era5():
    store = (
        "gs://gcp-public-data-arco-era5/ar/1959-2022-full_37-1h-0p25deg-chunk-1.zarr-v2"
    )
    ds = xr.open_zarr(
        store,
        chunks={"time": 48},
        consolidated=True,
    )
    ds["mask"] = ds.land_sea_mask
    return ds
