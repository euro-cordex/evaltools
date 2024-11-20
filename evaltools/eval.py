

def weighted_field_mean(ds, lon="rlon", lat="rlat", weights=None):
    """function to compute area-weighted spatial means"""
    if weights is None:
        weights = np.cos(np.deg2rad(ds[lat]))
    return ds.weighted(weights).mean(dim=(lon, lat))


def daily_sum(da):
    """Function to compute daily sums with a simple groupby approach"""
    return da.groupby("time.day").sum(dim="time")


def monthly_sum(da):
    """Function to compute dailymonthly means with a simple groupby approach"""
    return da.groupby("time.month").mean(dim="time")


def height_correction(height1, height2):
    """returns height correction in m"""
    return (height1 - height2) * 0.0065


def seasonal_mean(da):
    """Optimized function to calculate seasonal averages from time series of monthly means

    based on: https://xarray.pydata.org/en/stable/examples/monthly-means.html
    """

    # Get number od days for each month
    month_length = da.time.dt.days_in_month
    # Calculate the weights by grouping by 'time.season'.
    weights = (
        month_length.groupby("time.season") / month_length.groupby("time.season").sum()
    )

    # Test that the sum of the weights for each season is 1.0
    #np.testing.assert_allclose(weights.groupby("time.season").sum().values, np.ones(4))

    # Calculate the weighted average
    return (da * weights).groupby("time.season").sum(dim="time")


def get_regridder(finer, coarser, method="bilinear", **kwargs):
    """
    Function to regrid data bilinearly to a coarser grid
    """

    import xesmf as xe

    return xe.Regridder(finer, coarser, method=method, **kwargs)


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