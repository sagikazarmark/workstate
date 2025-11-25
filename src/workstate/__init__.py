from importlib.util import find_spec

from .file import FileLoader, FilePersister

__all__ = [
    "FileLoader",
    "FilePersister",
]

if find_spec("fsspec") is not None:
    from . import fsspec  # noqa: F401

    __all__.append("fsspec")

if find_spec("obstore") is not None:
    from . import obstore  # noqa: F401

    __all__.append("obstore")
