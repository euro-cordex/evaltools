# flake8: noqa
# This code was developed with the assistance of GitHub Copilot.

import xarray as xr
import numpy as np
import scipy.stats as stats

# from Kotlarski, S., Keuler, K., Christensen, O. B., Colette, A., Déqué, M., Gobiet, A., Goergen, K., Jacob,
# D., Lüthi, D., van Meijgaard, E., Nikulin, G., Schär, C., Teichmann, C., Vautard, R., Warrach-Sagi, K.,
# and Wulfmeyer, V.: Regional climate modeling on European scales: a joint standard evaluation of the
# EURO-CORDEX RCM ensemble, Geosci. Model Dev., 7, 1297–1333, https://doi.org/10.5194/gmd-7-1297-2014, 2014.

# BIAS: the difference (model − reference) of spatially
# averaged climatological annual or seasonal mean values
# for a selected subregion (relative difference for precipi-
# tation).

# Load the model and reference datasets
model_ds = xr.open_dataset("model_data.nc")
reference_ds = xr.open_dataset("reference_data.nc")

# Select the subregion of interest (example: lat/lon bounds)
subregion_model = model_ds.sel(lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max))
subregion_reference = reference_ds.sel(
    lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max)
)


def compute_bias(model_ds, reference_ds):
    """
    Compute the BIAS between model and reference data.

    The BIAS is the difference between the spatially averaged climatological annual or seasonal mean values
    of the model and reference datasets for a selected subregion. It quantifies the systematic error in the model
    data compared to the reference data.

    Parameters:
    model_ds (xarray.DataArray): The model data with spatial dimensions.
    reference_ds (xarray.DataArray): The reference data with spatial dimensions.

    Returns:
    xarray.DataArray: The BIAS value.
    """
    # Compute the climatological annual or seasonal mean values
    model_clim = model_ds.groupby("time.season").mean("time")
    reference_clim = reference_ds.groupby("time.season").mean("time")

    # Spatially average these mean values over the subregion
    model_spatial_avg = model_clim.mean(dim=["lat", "lon"])
    reference_spatial_avg = reference_clim.mean(dim=["lat", "lon"])

    # Calculate the difference (BIAS)
    bias = model_spatial_avg - reference_spatial_avg

    return bias


# 95 %-P: the 95th percentile of all absolute grid cell dif-
# ferences (model − reference) across a selected subre-
# gion based on climatological annual or seasonal mean
# values (relative difference for precipitation).

# Compute the climatological annual or seasonal mean values
model_clim = subregion_model.groupby("time.season").mean("time")
reference_clim = subregion_reference.groupby("time.season").mean("time")


def compute_95th_percentile(model_ds, reference_ds):
    """
    Compute the 95th percentile of all absolute grid cell differences between model and reference data.

    This metric calculates the 95th percentile of the absolute differences between the model and reference
    datasets across all grid points of a selected subregion, based on climatological annual or seasonal mean values.
    It provides a measure of the extreme differences between the model and reference data, highlighting the
    largest discrepancies.

    Parameters:
    model_ds (xarray.DataArray): The model data with spatial dimensions.
    reference_ds (xarray.DataArray): The reference data with spatial dimensions.

    Returns:
    xarray.DataArray: The 95th percentile of the absolute differences.
    """
    absolute_diff = np.abs(model_ds - reference_ds)
    return absolute_diff.quantile(0.95, dim=["lat", "lon"])


# PACO: the spatial pattern correlation between climato-
# logical annual or seasonal mean values of model and
# reference data across all grid points of a selected subre-
# gion.

# Compute the climatological annual or seasonal mean values
model_clim = subregion_model.groupby("time.season").mean("time")
reference_clim = subregion_reference.groupby("time.season").mean("time")

# Flatten the spatial dimensions to compute the correlation
model_flat = model_clim.stack(grid_points=("lat", "lon"))
reference_flat = reference_clim.stack(grid_points=("lat", "lon"))


# Calculate the spatial pattern correlation
def spatial_pattern_correlation(model, reference):
    """
    Calculate the spatial pattern correlation between model and reference data.

    The spatial pattern correlation (PACO) measures the similarity in the spatial distribution
    of climatological mean values between the model and reference datasets across a selected subregion.
    It quantifies how well the spatial patterns (i.e., the arrangement and variation of values across
    different grid points) in the model data match those in the reference data.

    The correlation value ranges from -1 to 1:
    - A value of 1 indicates a perfect positive correlation, meaning the spatial patterns in the model
      and reference datasets are identical.
    - A value of -1 indicates a perfect negative correlation, meaning the spatial patterns are exactly opposite.
    - A value of 0 indicates no correlation, meaning there is no linear relationship between the spatial patterns
      in the model and reference datasets.

    The spatial pattern correlation provides insights into:
    - Model Performance: A higher positive correlation value suggests that the model is accurately capturing
      the spatial distribution of the climatological mean values as observed in the reference data. Conversely,
      a lower or negative correlation value indicates that the model's spatial patterns deviate significantly
      from those in the reference data.
    - Spatial Agreement: The spatial pattern correlation helps in assessing the agreement between the model
      and reference data in terms of spatial variability. It is particularly useful for evaluating the model's
      ability to reproduce spatial features such as gradients, hotspots, and other spatial structures.

    Parameters:
    model (xarray.DataArray): The model data with spatial dimensions flattened.
    reference (xarray.DataArray): The reference data with spatial dimensions flattened.

    Returns:
    float: The spatial pattern correlation value.
    """
    model_mean = model.mean("grid_points")
    reference_mean = reference.mean("grid_points")
    covariance = ((model - model_mean) * (reference - reference_mean)).mean(
        "grid_points"
    )
    model_std = model.std("grid_points")
    reference_std = reference.std("grid_points")
    correlation = covariance / (model_std * reference_std)
    return correlation


paco = spatial_pattern_correlation(model_flat, reference_flat)

# RSV: ratio (model over reference) of spatial standard de-
# viations across all grid points of a selected subregion of
# climatological annual or seasonal mean values.

# Compute the climatological annual or seasonal mean values
model_clim = subregion_model.groupby("time.season").mean("time")
reference_clim = subregion_reference.groupby("time.season").mean("time")


def compute_rsv(model_ds, reference_ds):
    """
    Compute the Ratio of Spatial Variability (RSV) between model and reference data.

    The Ratio of Spatial Variability (RSV) is the ratio of the spatial standard deviations
    of climatological annual or seasonal mean values between the model and reference datasets
    across all grid points of a selected subregion. It quantifies the relative spatial variability
    in the model data compared to the reference data.

    An RSV value greater than 1 indicates that the model data has higher spatial variability
    than the reference data, while an RSV value less than 1 indicates lower spatial variability
    in the model data compared to the reference data.

    Parameters:
    model_ds (xarray.DataArray): The model data with spatial dimensions.
    reference_ds (xarray.DataArray): The reference data with spatial dimensions.

    Returns:
    xarray.DataArray: The RSV value.
    """
    model_std = model_ds.std(dim=["lat", "lon"])
    reference_std = reference_ds.std(dim=["lat", "lon"])
    return model_std / reference_std


# TCOIAV: temporal correlation of interannual variabil-
# ity between model and reference time series of spatially
# averaged annual or seasonal mean values of a selected
# subregion.


def compute_tcoiav(model_ds, reference_ds):
    """
    Compute the Temporal Correlation of Interannual Variability (TCOIAV) between model and reference data.

    The Temporal Correlation of Interannual Variability (TCOIAV) measures the correlation between the
    interannual variability of spatially averaged annual or seasonal mean values of the model and reference
    datasets for a selected subregion. It quantifies how well the model captures the year-to-year variations
    observed in the reference data.

    A higher positive TCOIAV value indicates that the model accurately captures the interannual variability
    observed in the reference data, while a lower or negative TCOIAV value indicates that the model's
    interannual variability deviates significantly from that of the reference data.

    Parameters:
    model_ds (xarray.DataArray): The model data with spatial dimensions.
    reference_ds (xarray.DataArray): The reference data with spatial dimensions.

    Returns:
    float: The TCOIAV value.
    """
    # Compute the annual or seasonal mean values
    model_mean = model_ds.groupby("time.year").mean("time")
    reference_mean = reference_ds.groupby("time.year").mean("time")

    # Spatially average these mean values over the subregion
    model_spatial_avg = model_mean.mean(dim=["lat", "lon"])
    reference_spatial_avg = reference_mean.mean(dim=["lat", "lon"])

    # Compute the temporal correlation of interannual variability
    tcoiav = xr.corr(model_spatial_avg, reference_spatial_avg, dim="year")

    return tcoiav


# RIAV: ratio (model over reference) of temporal standard
# deviations of interannual time series of spatially aver-
# aged annual or seasonal mean values of a selected sub-
# region.


def compute_riav(model_ds, reference_ds):
    """
    Compute the Ratio of Interannual Variability (RIAV) between model and reference data.

    The Ratio of Interannual Variability (RIAV) is the ratio of the temporal standard deviations
    of interannual time series of spatially averaged annual or seasonal mean values between the model
    and reference datasets for a selected subregion. It quantifies the relative temporal variability
    in the model data compared to the reference data.

    An RIAV value greater than 1 indicates that the model data has higher temporal variability
    than the reference data, while an RIAV value less than 1 indicates lower temporal variability
    in the model data compared to the reference data.

    Parameters:
    model_ds (xarray.DataArray): The model data with spatial dimensions.
    reference_ds (xarray.DataArray): The reference data with spatial dimensions.

    Returns:
    float: The RIAV value.
    """
    # Compute the annual or seasonal mean values
    model_mean = model_ds.groupby("time.year").mean("time")
    reference_mean = reference_ds.groupby("time.year").mean("time")

    # Spatially average these mean values over the subregion
    model_spatial_avg = model_mean.mean(dim=["lat", "lon"])
    reference_spatial_avg = reference_mean.mean(dim=["lat", "lon"])

    # Compute the temporal standard deviations of the interannual time series
    model_std = model_spatial_avg.std(dim="year")
    reference_std = reference_spatial_avg.std(dim="year")

    # Compute the ratio of these standard deviations (RIAV)
    riav = model_std / reference_std

    return riav


# CRCO: Spearman rank correlation between spatially av-
# eraged monthly values of model and reference data of
# the climatological mean annual cycle of a selected sub-
# region.


def compute_crco(model_ds, reference_ds):
    """
    Compute the Spearman Rank Correlation (CRCO) between spatially averaged monthly values of model and reference data.

    The Spearman Rank Correlation (CRCO) measures the correlation between the spatially averaged monthly values
    of the model and reference data of the climatological mean annual cycle for a selected subregion. It quantifies
    how well the model captures the monthly variations observed in the reference data.

    A higher positive CRCO value indicates that the model accurately captures the monthly variations observed
    in the reference data, while a lower or negative CRCO value indicates that the model's monthly variations
    deviate significantly from those of the reference data.

    Parameters:
    model_ds (xarray.DataArray): The model data with spatial dimensions.
    reference_ds (xarray.DataArray): The reference data with spatial dimensions.

    Returns:
    float: The CRCO value.
    """
    # Compute the monthly mean values
    model_monthly_mean = model_ds.groupby("time.month").mean("time")
    reference_monthly_mean = reference_ds.groupby("time.month").mean("time")

    # Spatially average these mean values over the subregion
    model_spatial_avg = model_monthly_mean.mean(dim=["lat", "lon"])
    reference_spatial_avg = reference_monthly_mean.mean(dim=["lat", "lon"])

    # Compute the climatological mean annual cycle
    model_clim_mean_annual_cycle = model_spatial_avg.groupby("month").mean()
    reference_clim_mean_annual_cycle = reference_spatial_avg.groupby("month").mean()

    # Compute the Spearman rank correlation
    spearman_corr, _ = stats.spearmanr(
        model_clim_mean_annual_cycle, reference_clim_mean_annual_cycle
    )

    return spearman_corr


# ROYA: ratio (model over reference) of yearly amplitudes
# (differences between maximum and minimum) of spa-
# tially averaged monthly values of the climatological
# mean annual cycle of a selected subregion.


def compute_roya(model_ds, reference_ds):
    """
    Compute the Ratio of Yearly Amplitudes (ROYA) between model and reference data.

    The Ratio of Yearly Amplitudes (ROYA) is the ratio of the yearly amplitudes (differences between maximum
    and minimum) of spatially averaged monthly values of the climatological mean annual cycle between the model
    and reference datasets for a selected subregion. It quantifies the relative amplitude of the annual cycle
    in the model data compared to the reference data.

    A ROYA value greater than 1 indicates that the model data has a higher yearly amplitude than the reference data,
    while a ROYA value less than 1 indicates a lower yearly amplitude in the model data compared to the reference data.

    Parameters:
    model_ds (xarray.DataArray): The model data with spatial dimensions.
    reference_ds (xarray.DataArray): The reference data with spatial dimensions.

    Returns:
    float: The ROYA value.
    """
    # Compute the monthly mean values
    model_monthly_mean = model_ds.groupby("time.month").mean("time")
    reference_monthly_mean = reference_ds.groupby("time.month").mean("time")

    # Spatially average these mean values over the subregion
    model_spatial_avg = model_monthly_mean.mean(dim=["lat", "lon"])
    reference_spatial_avg = reference_monthly_mean.mean(dim=["lat", "lon"])

    # Compute the climatological mean annual cycle
    model_clim_mean_annual_cycle = model_spatial_avg.groupby("month").mean()
    reference_clim_mean_annual_cycle = reference_spatial_avg.groupby("month").mean()

    # Calculate the yearly amplitude (difference between maximum and minimum)
    model_amplitude = (
        model_clim_mean_annual_cycle.max() - model_clim_mean_annual_cycle.min()
    )
    reference_amplitude = (
        reference_clim_mean_annual_cycle.max() - reference_clim_mean_annual_cycle.min()
    )

    # Compute the ratio of these amplitudes (ROYA)
    roya = model_amplitude / reference_amplitude
