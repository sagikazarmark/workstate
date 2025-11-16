import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Generic, TypeVar

import obstore

from .filter import filter_files
from .options import (
    HasFilter,
    HasPrefix,
    HasUrl,
    MayHaveFilter,
    MayHavePrefix,
    MayHaveUrl,
    Prefix,
    resolve_filter,
    resolve_prefix,
)

Options = TypeVar(
    "Options",
    bound=HasPrefix | HasUrl | HasFilter | MayHavePrefix | MayHaveUrl | MayHaveFilter,
    contravariant=True,
)


class StateManager(Generic[Options]):
    def __init__(
        self,
        store: obstore.store.ObjectStore,
        # logger: logging.Logger = _logger,
    ):
        self.store: obstore.store.ObjectStore = store
        # self.logger: logging.Logger = logging.getLogger(__name__)

    @contextmanager
    def save(
        self,
        options: Options,
    ) -> Iterator[str]:
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir

            prefix = resolve_prefix(options)

            for file_path in filter_files(Path(temp_dir), resolve_filter(options)):
                relative_path = file_path.relative_to(temp_dir)
                object_key = str(prefix.joinpath(relative_path))

                # logger.info("Uploading file", extra={"file": str(relative_path)})

                self.store.put(object_key, file_path)


__all__ = [
    "HasFilter",
    "HasPrefix",
    "HasUrl",
    "MayHaveFilter",
    "MayHavePrefix",
    "MayHaveUrl",
    "Prefix",
    "resolve_filter",
    "resolve_prefix",
    "StateManager",
]
