# flake8: noqa

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

# Compute the climatological annual or seasonal mean values
model_clim = subregion_model.groupby("time.season").mean("time")
reference_clim = subregion_reference.groupby("time.season").mean("time")

# Spatially average these mean values over the subregion
model_spatial_avg = model_clim.mean(dim=["lat", "lon"])
reference_spatial_avg = reference_clim.mean(dim=["lat", "lon"])

# Calculate the difference (BIAS)
bias = model_spatial_avg - reference_spatial_avg

# 95 %-P: the 95th percentile of all absolute grid cell dif-
# ferences (model − reference) across a selected subre-
# gion based on climatological annual or seasonal mean
# values (relative difference for precipitation).

# Load the model and reference datasets
model_ds = xr.open_dataset("model_data.nc")
reference_ds = xr.open_dataset("reference_data.nc")

# Select the subregion of interest (example: lat/lon bounds)
subregion_model = model_ds.sel(lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max))
subregion_reference = reference_ds.sel(
    lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max)
)

# Compute the climatological annual or seasonal mean values
model_clim = subregion_model.groupby("time.season").mean("time")
reference_clim = subregion_reference.groupby("time.season").mean("time")

# Calculate the absolute differences
absolute_diff = np.abs(model_clim - reference_clim)

# Compute the 95th percentile of these absolute differences
percentile_95 = absolute_diff.quantile(0.95, dim=["lat", "lon"])

# PACO: the spatial pattern correlation between climato-
# logical annual or seasonal mean values of model and
# reference data across all grid points of a selected subre-
# gion.

# Load the model and reference datasets
model_ds = xr.open_dataset("model_data.nc")
reference_ds = xr.open_dataset("reference_data.nc")

# Select the subregion of interest (example: lat/lon bounds)
subregion_model = model_ds.sel(lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max))
subregion_reference = reference_ds.sel(
    lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max)
)

# Compute the climatological annual or seasonal mean values
model_clim = subregion_model.groupby("time.season").mean("time")
reference_clim = subregion_reference.groupby("time.season").mean("time")

# Flatten the spatial dimensions to compute the correlation
model_flat = model_clim.stack(grid_points=("lat", "lon"))
reference_flat = reference_clim.stack(grid_points=("lat", "lon"))


# Calculate the spatial pattern correlation
def spatial_pattern_correlation(model, reference):
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

# Load the model and reference datasets
model_ds = xr.open_dataset("model_data.nc")
reference_ds = xr.open_dataset("reference_data.nc")

# Select the subregion of interest (example: lat/lon bounds)
subregion_model = model_ds.sel(lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max))
subregion_reference = reference_ds.sel(
    lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max)
)

# Compute the climatological annual or seasonal mean values
model_clim = subregion_model.groupby("time.season").mean("time")
reference_clim = subregion_reference.groupby("time.season").mean("time")

# Calculate the spatial standard deviations
model_std = model_clim.std(dim=["lat", "lon"])
reference_std = reference_clim.std(dim=["lat", "lon"])

# Compute the ratio of these standard deviations (RSV)
rsv = model_std / reference_std

# TCOIAV: temporal correlation of interannual variabil-
# ity between model and reference time series of spatially
# averaged annual or seasonal mean values of a selected
# subregion.

# Load the model and reference datasets
model_ds = xr.open_dataset("model_data.nc")
reference_ds = xr.open_dataset("reference_data.nc")

# Select the subregion of interest (example: lat/lon bounds)
subregion_model = model_ds.sel(lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max))
subregion_reference = reference_ds.sel(
    lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max)
)

# Compute the annual or seasonal mean values
model_mean = subregion_model.groupby("time.year").mean("time")
reference_mean = subregion_reference.groupby("time.year").mean("time")

# Spatially average these mean values over the subregion
model_spatial_avg = model_mean.mean(dim=["lat", "lon"])
reference_spatial_avg = reference_mean.mean(dim=["lat", "lon"])

# Compute the temporal correlation of interannual variability
tcoiav = xr.corr(model_spatial_avg, reference_spatial_avg, dim="year")

# RIAV: ratio (model over reference) of temporal standard
# deviations of interannual time series of spatially aver-
# aged annual or seasonal mean values of a selected sub-
# region.

# Load the model and reference datasets
model_ds = xr.open_dataset("model_data.nc")
reference_ds = xr.open_dataset("reference_data.nc")

# Select the subregion of interest (example: lat/lon bounds)
subregion_model = model_ds.sel(lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max))
subregion_reference = reference_ds.sel(
    lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max)
)

# Compute the annual or seasonal mean values
model_mean = subregion_model.groupby("time.year").mean("time")
reference_mean = subregion_reference.groupby("time.year").mean("time")

# Spatially average these mean values over the subregion
model_spatial_avg = model_mean.mean(dim=["lat", "lon"])
reference_spatial_avg = reference_mean.mean(dim=["lat", "lon"])

# Compute the temporal standard deviations of the interannual time series
model_std = model_spatial_avg.std(dim="year")
reference_std = reference_spatial_avg.std(dim="year")

# Compute the ratio of these standard deviations (RIAV)
riav = model_std / reference_std

# CRCO: Spearman rank correlation between spatially av-
# eraged monthly values of model and reference data of
# the climatological mean annual cycle of a selected sub-
# region.

# Load the model and reference datasets
model_ds = xr.open_dataset("model_data.nc")
reference_ds = xr.open_dataset("reference_data.nc")

# Select the subregion of interest (example: lat/lon bounds)
subregion_model = model_ds.sel(lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max))
subregion_reference = reference_ds.sel(
    lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max)
)

# Compute the monthly mean values
model_monthly_mean = subregion_model.groupby("time.month").mean("time")
reference_monthly_mean = subregion_reference.groupby("time.month").mean("time")

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

# ROYA: ratio (model over reference) of yearly amplitudes
# (differences between maximum and minimum) of spa-
# tially averaged monthly values of the climatological
# mean annual cycle of a selected subregion.

# Load the model and reference datasets
model_ds = xr.open_dataset("model_data.nc")
reference_ds = xr.open_dataset("reference_data.nc")

# Select the subregion of interest (example: lat/lon bounds)
subregion_model = model_ds.sel(lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max))
subregion_reference = reference_ds.sel(
    lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max)
)

# Compute the monthly mean values
model_monthly_mean = subregion_model.groupby("time.month").mean("time")
reference_monthly_mean = subregion_reference.groupby("time.month").mean("time")

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
