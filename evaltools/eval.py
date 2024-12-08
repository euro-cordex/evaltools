import numpy as np
import xarray as xr


def regional_mean(ds, regions):
    """
    Compute the regional mean of a dataset over specified regions.

    Parameters:
    ds (xarray.Dataset): The dataset to compute the regional mean for.
    regions (regionmask.Regions): The regions to compute the mean over.

    Returns:
    xarray.Dataset: The regional mean values.
    """
    mask = regions.mask_3D(ds.lon, ds.lat, drop=False)
    weights = 1.0  # np.cos(np.deg2rad(ds.lat))
    return ds.cf.weighted(mask * weights).mean(dim=("X", "Y"))


def regional_means(dsets, regions):
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


def seasonal_mean(da):
    """
    Calculate seasonal averages from a time series of monthly means.

    Parameters:
    da (xarray.DataArray): The DataArray to compute seasonal means for.

    Returns:
    xarray.DataArray: The seasonal mean values.
    """
    # Get number of days for each month
    month_length = da.time.dt.days_in_month
    # Calculate the weights by grouping by 'time.season'.
    weights = (
        month_length.groupby("time.season") / month_length.groupby("time.season").sum()
    )

    # Test that the sum of the weights for each season is 1.0
    # np.testing.assert_allclose(weights.groupby("time.season").sum().values, np.ones(4))

    # Calculate the weighted average
    return (da * weights).groupby("time.season").sum(dim="time")


def get_regridder(finer, coarser, method="bilinear", **kwargs):
    """
    Regrid data bilinearly to a coarser grid.

    Parameters:
    finer (xarray.Dataset): The dataset to regrid.
    coarser (xarray.Dataset): The target grid dataset.
    method (str, optional): The regridding method to use. Defaults to "bilinear".
    **kwargs: Additional keyword arguments to pass to the regridding function.

    Returns:
    xesmf.Regridder: The regridder object.
    """
    import xesmf as xe

    regridder = xe.Regridder(finer, coarser, method, **kwargs)
    return regridder


def compare_seasons(
    ds1, ds2, regrid="ds1", do_height_correction=False, orog1=None, orog2=None
):
    """
    Function to compare seasonal means of two datasets.

    Paramters
    ---------
    ds1 : xarray.Dataset
        First variable data for comparision. Temporal resolution has to be less than monthly.
        ds1 is mainly model output data.
        ds2 is subtracted from ds1.
    ds2 : xarray.Dataset
        Second variable data for comparision. Temporal resolution has to be less than monthly.
        ds2 is mainly observational or reanalysis data.
        ds2 is subtracted from ds1.
    regrid : {"ds1", "ds2"}, optional
        Denotes the dataset to be bilinearly regridded. Specify the dataset with the finer spatial resolution:

        - "ds1": Regrid ds1 to ds2's grid with coarser spatial resolution.
        - "ds2": Regrid ds2 to ds1's grid with coarser spatial resolution.
    do_height_correction : bool, optional
        If ``do_height_correction=True``, do a height correction on ds1 using two orography files orog1 and orog2.
    orog1 : xarray.Dataset, optional
        Use only if ``do_height_correction=True``.
        Specify a orography file referring to ds1.
    orog2 : xarray.Dataset, optional
        Use only if ``do_height_correction=True``.
        Specify a orography file referring to ds2.

    Returns
    -------
    seasonal_comparision : xarray.Dataset
        Spatial mean differences of two datasets

    """
    ds1 = ds1.copy()
    ds2 = ds2.copy()
    ds1_seasmean = seasonal_mean(ds1)
    ds2_seasmean = seasonal_mean(ds2)
    if regrid == "ds1":
        regridder = get_regridder(ds1, ds2)
        print(regridder)
        ds1_seasmean = regridder(ds1_seasmean)
    elif regrid == "ds2":
        regridder = get_regridder(ds2, ds1)
        print(regridder)
        ds2_seasmean = regridder(ds2_seasmean)

    if do_height_correction is True:
        orog1 = regridder(orog1)
        ds1_seasmean += height_correction(orog1, orog2)
    return ds1_seasmean - ds2_seasmean
    # return xr.where(ds1_seasmean.mask, ds2_seasmean - ds1_seasmean, np.nan)
