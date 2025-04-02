import cf_xarray as cfxr  # noqa
import warnings

grid_mapping_varname = "crs"


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


def check_grid_mapping(ds, iid=None):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")  # Catch all warnings
        grid_mapping_name = ds.cf["grid_mapping"].attrs.get(
            "grid_mapping_name"
        )  # Call the function

        # Check if any warnings were raised
        if w:
            for warning in w:
                print(f"Warning for {iid}: {warning.message}")

    grid_mapping_varname = ds.cf["grid_mapping"].name
    if grid_mapping_name not in [
        "rotated_latitude_longitude",
        "lambert_conformal_conic",
    ]:
        warnings.warn(f"Grid mapping name {grid_mapping_name} is not supported")
        raise FixException(f"Grid mapping name {grid_mapping_name} is not supported")
    if grid_mapping_varname != "crs":
        print(f"Renaming grid mapping variable {grid_mapping_varname} to 'crs'")
        ds = update_grid_mapping_varname(ds)
    return ds


def check_and_fix(ds, iid=None):
    ds = ds.copy()
    # Check if the grid mapping variable is named 'crs'
    ds = check_grid_mapping(ds, iid)
    return ds
