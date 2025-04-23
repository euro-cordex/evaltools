import cf_xarray as cfxr  # noqa
import cordex as cx
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
        warnings.warn(
            f"Grid mapping name {grid_mapping_name} is not supported for {iid}"
        )
        raise FixException(
            f"Grid mapping name {grid_mapping_name} is not supported for {iid}"
        )
    if grid_mapping_varname != "crs":
        print(
            f"Renaming grid mapping variable {grid_mapping_varname} to 'crs' for {iid}"
        )
        ds = update_grid_mapping_varname(ds)
    if grid_mapping_name == "rotated_latitude_longitude":
        # check if attributes are set correctly
        domain_id = ds.cx.domain_id
        domain_info = cx.domain_info(domain_id)
        pollon = ds.cf["grid_mapping"].attrs.get("grid_north_pole_longitude")
        pollat = ds.cf["grid_mapping"].attrs.get("grid_north_pole_latitude")
        if pollon != domain_info["pollon"] or pollat != domain_info["pollat"]:
            message = f"Grid mapping has ({pollon}, {pollat}) which is inconsistent with ({domain_info['pollon']}, {domain_info['pollat']}) for {domain_id} and {iid}."
            warnings.warn(message)
            raise FixException(message)
    return ds


def check_and_fix(ds, iid=None):
    ds = ds.copy()
    # Check if the grid mapping variable is named 'crs'
    ds = check_grid_mapping(ds, iid)
    return ds
