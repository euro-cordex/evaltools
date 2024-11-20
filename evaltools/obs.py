import xarray as xr


def eobs():
    """open EOBS dataset from LEAP
    
    See also: https://catalog.leap.columbia.edu/feedstock/eobs-dataset
    
    """
    store = "https://ncsa.osn.xsede.org/Pangeo/pangeo-forge/pangeo-forge/EOBS-feedstock/eobs-tg-tn-tx-rr-hu-pp.zarr"

    return xr.open_dataset(store, engine="zarr", chunks={})
