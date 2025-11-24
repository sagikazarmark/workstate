from __future__ import annotations

from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Sequence, overload

import obstore
from pydantic import AnyUrl, DirectoryPath

from ..directory import PathFilter, PrefixFilter, _filter_files
from .base import _Base

if TYPE_CHECKING:
    from obstore import ListStream, ObjectMeta


class _DirectoryBase(_Base):
    pass


class DirectoryLoader(_DirectoryBase):
    @overload
    def load(self, ref: AnyUrl | PurePosixPath, dst: DirectoryPath): ...

    @overload
    def load(
        self,
        ref: AnyUrl | PurePosixPath,
        dst: DirectoryPath,
        filter: PrefixFilter,
    ): ...

    def load(
        self,
        ref: AnyUrl | PurePosixPath,
        dst: DirectoryPath,
        filter: PrefixFilter | None = None,
    ):
        store, path = self._resolve_store_and_prefix(ref)

        # Handle empty path - convert "." to None for obstore.list
        prefix_str = str(path) if path is not None else None

        stream: ListStream[Sequence[ObjectMeta]] = obstore.list(
            store,
            prefix=prefix_str,
        )

        for objects in stream:
            for obj in objects:
                obj_full_key = obj.path  # Keep original for download
                obj_path = PurePosixPath(obj_full_key)

                # Strip the prefix to get relative path for filtering and local storage
                if path is not None and obj_path.is_relative_to(path):
                    obj_relative_path = obj_path.relative_to(path)
                elif path is None:
                    # No prefix, use the full path as relative
                    obj_relative_path = obj_path
                else:
                    # This shouldn't happen if prefix filtering works correctly
                    continue

                # Apply filter to the relative path
                if filter and not filter.match(obj_relative_path):
                    continue

                # Save to local path using relative structure
                dst_file = dst / obj_relative_path
                dst_file.parent.mkdir(parents=True, exist_ok=True)

                # Download using the full object key
                data = obstore.get(store, obj_full_key).bytes().to_bytes()
                dst_file.write_bytes(data)


class DirectoryPersister(_DirectoryBase):
    @overload
    def persist(
        self,
        ref: AnyUrl | PurePosixPath,
        src: DirectoryPath,
    ): ...

    @overload
    def persist(
        self,
        ref: AnyUrl | PurePosixPath,
        src: DirectoryPath,
        filter: PathFilter,
    ): ...

    def persist(
        self,
        ref: AnyUrl | PurePosixPath,
        src: DirectoryPath,
        filter: PathFilter | None = None,
    ):
        store, path = self._resolve_store_and_prefix(ref)

        for file_path in _filter_files(src, filter):
            relative_path = file_path.relative_to(src)
            if path is not None:
                object_key = str(path.joinpath(relative_path))
            else:
                object_key = str(relative_path)

            # self.logger.info("Uploading file", extra={"file": str(relative_path)})

            store.put(object_key, file_path)
