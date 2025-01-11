import numpy as np
import xarray as xr
import cordex as cx
import cf_xarray as cfxr
import xesmf as xe
from warnings import warn


def regional_mean(ds, regions=None, weights=None):
    """
    Compute the regional mean of a dataset over specified regions.

    Parameters:
    ds (xarray.Dataset): The dataset to compute the regional mean for.
    regions (regionmask.Regions): The regions to compute the mean over.

    Returns:
    xarray.Dataset: The regional mean values.
    """
    mask = 1.0
    if weights is None:
        weights = xr.ones_like(ds.lon)
    if regions:
        mask = regions.mask_3D(ds.lon, ds.lat, drop=False)

    return ds.cf.weighted(mask * weights).mean(dim=("X", "Y"))


def regional_means(dsets, regions=None):
    """
    Compute the regional means for multiple datasets over specified regions.

    Parameters:
    dsets (dict): A dictionary of datasets to compute the regional means for.
    regions (regionmask.Regions): The regions to compute the means over.

    Returns:
    xarray.Dataset: The concatenated regional mean values for all datasets.
    """
    concat_dim = xr.DataArray(list(dsets.keys()), dims="iid", name="iid")
    return xr.concat(
        [regional_mean(ds, regions) for ds in dsets.values()],
        dim=concat_dim,
        coords="minimal",
        compat="override",
    )


def weighted_field_mean(ds, lon="rlon", lat="rlat", weights=None):
    """
    Compute the area-weighted spatial mean of a dataset.

    Parameters:
    ds (xarray.Dataset): The dataset to compute the weighted mean for.
    lon (str, optional): The name of the longitude dimension. Defaults to "rlon".
    lat (str, optional): The name of the latitude dimension. Defaults to "rlat".
    weights (xarray.DataArray, optional): The weights to use for the mean. Defaults to None.

    Returns:
    xarray.Dataset: The area-weighted spatial mean values.
    """
    if weights is None:
        weights = np.cos(np.deg2rad(ds[lat]))
    return ds.weighted(weights).mean(dim=(lon, lat))


def daily_sum(da):
    """
    Compute daily sums of a DataArray using a simple groupby approach.

    Parameters:
    da (xarray.DataArray): The DataArray to compute daily sums for.

    Returns:
    xarray.DataArray: The daily sum values.
    """
    return da.groupby("time.day").sum(dim="time")


def monthly_sum(da):
    """
    Compute monthly means of a DataArray using a simple groupby approach.

    Parameters:
    da (xarray.DataArray): The DataArray to compute monthly means for.

    Returns:
    xarray.DataArray: The monthly mean values.
    """
    return da.groupby("time.month").mean(dim="time")


def height_correction(height1, height2):
    """
    Compute the height correction in meters between two heights.

    Parameters:
    height1 (float): The first height in meters.
    height2 (float): The second height in meters.

    Returns:
    float: The height correction in meters.
    """
    return (height1 - height2) * 0.0065


def seasonal_mean(da, skipna=True, min_count=1):
    """Calculate seasonal averages from time series of monthly means

    based on: https://xarray.pydata.org/en/stable/examples/monthly-means.html
    """
    # Get number od days for each month
    month_length = da.time.dt.days_in_month
    # Calculate the weights by grouping by 'time.season'.
    weights = (
        month_length.groupby("time.season") / month_length.groupby("time.season").sum()
    )

    # Test that the sum of the weights for each season is 1.0
    # np.testing.assert_allclose(weights.groupby("time.season").sum().values, np.ones(4))

    # Calculate the weighted average
    return (
        (da * weights)
        .groupby("time.season")
        .sum(dim="time", skipna=skipna, min_count=min_count)
    )


def add_bounds(ds):
    if "longitude" not in ds.cf.bounds and "latitude" not in ds.cf.bounds:
        ds = cx.transform_bounds(ds, trg_dims=("vertices_lon", "vertices_lat"))
    ds = ds.assign_coords(
        lon_b=cfxr.bounds_to_vertices(
            ds.vertices_lon, bounds_dim="vertices", order="counterclockwise"
        ),
        lat_b=cfxr.bounds_to_vertices(
            ds.vertices_lat, bounds_dim="vertices", order="counterclockwise"
        ),
    )
    return ds


def mask_with_sftlf(ds, sftlf=None):
    if sftlf is None and "sftlf" in ds:
        sftlf = ds["sftlf"]
        for var in ds.data_vars:
            if var != "sftlf":
                ds[var] = ds[var].where(sftlf > 0)
        ds["mask"] = sftlf > 0
    else:
        warn(f"sftlf not found in dataset: {ds.source_id}")
    return ds


def create_cordex_grid(domain_id):
    """
    Creates a CORDEX grid for the specified domain.

    Parameters
    ----------
    domain_id : str
        The domain ID for the CORDEX grid.

    Returns
    -------
    xarray.Dataset
        The CORDEX grid with assigned coordinates for longitude and latitude bounds.
    """
    grid = cx.domain(domain_id, bounds=True, mip_era="CMIP6")
    lon_b = cfxr.bounds_to_vertices(
        grid.vertices_lon, bounds_dim="vertices", order="counterclockwise"
    )
    lat_b = cfxr.bounds_to_vertices(
        grid.vertices_lat, bounds_dim="vertices", order="counterclockwise"
    )
    return grid.assign_coords(lon_b=lon_b, lat_b=lat_b)


def create_regridder(source, target, method="bilinear"):
    """
    Creates a regridder for regridding data from the source grid to the target grid.

    Parameters
    ----------
    source : xarray.Dataset
        The source dataset to be regridded.
    target : xarray.Dataset
        The target grid dataset.
    method : str, optional
        The regridding method to use. Default is "bilinear".

    Returns
    -------
    xesmf.Regridder
        The regridder object.
    """
    regridder = xe.Regridder(source, target, method=method)
    return regridder


def regrid(ds, regridder):
    """
    Regrids the dataset using the specified regridder.

    Parameters
    ----------
    ds : xarray.Dataset
        The dataset to be regridded.
    regridder : xesmf.Regridder
        The regridder object.

    Returns
    -------
    xarray.Dataset
        The regridded dataset.
    """
    ds_regrid = regridder(ds)
    for var in ds.data_vars:
        if var not in ["mask", "sftlf"]:
            ds_regrid[var] = ds_regrid[var].where(ds_regrid["mask"] > 0.0)
    return ds_regrid


def regrid_dsets(dsets, target_grid, method="bilinear"):
    """
    Regrids multiple datasets to the target grid.

    Parameters
    ----------
    dsets : dict
        A dictionary of datasets to be regridded, with keys as dataset IDs and values as xarray.Datasets.
    target_grid : xarray.Dataset
        The target grid dataset.
    method : str, optional
        The regridding method to use. Default is "bilinear".

    Returns
    -------
    dict
        A dictionary of regridded datasets.
    """
    for dset_id, ds in dsets.items():
        print(dset_id)
        mapping = ds.cf["grid_mapping"].grid_mapping_name
        if mapping == "rotated_latitude_longitude":
            dsets[dset_id] = ds.cx.rewrite_coords(coords="all")
        else:
            print(f"regridding {dset_id} with grid_mapping: {mapping}")
            regridder = create_regridder(ds, target_grid, method=method)
            print(regridder)
            dsets[dset_id] = regrid(ds, regridder)
    return dsets
