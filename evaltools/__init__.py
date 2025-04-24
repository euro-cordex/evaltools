from importlib.metadata import version as _get_version

try:
    __version__ = _get_version("evaltools")
except Exception:  # pragma: no cover
    __version__ = "999"
