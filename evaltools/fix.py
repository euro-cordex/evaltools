import cf_xarray as cfxr  # noqa
from warnings import warn


class FixException(Exception):
    """
    Custom exception for errors encountered during dataset fixes.
    """

    def __init__(self, message):
        super().__init__(message)


def update_grid_mapping_varname(ds, varname="crs"):
    """
    Update the grid mapping variable name to 'crs' if it is not already set.
    """
    ds = ds.rename({ds.cf["grid_mapping"].name: "crs"})
    for var in ds.data_vars:
        if ds[var].attrs.get("grid_mapping"):
            ds[var].attrs["grid_mapping"] = "crs"
    return ds


def check_grid_mapping(ds):
    grid_mapping_name = ds.cf["grid_mapping"].attrs.get("grid_mapping_name")
    grid_mapping_varname = ds.cf["grid_mapping"].name
    if grid_mapping_name not in [
        "rotated_latitude_longitude",
        "lambert_conformal_conic",
    ]:
        warn(f"Grid mapping name {grid_mapping_name} is not supported")
        raise FixException(f"Grid mapping name {grid_mapping_name} is not supported")
    if grid_mapping_varname != "crs":
        print(f"Renaming grid mapping variable {grid_mapping_varname} to 'crs'")
        ds = update_grid_mapping_varname(ds)
    return ds


def check_and_fix(ds):
    ds = ds.copy()
    # Check if the grid mapping variable is named 'crs'
    ds = check_grid_mapping(ds)
    return ds
