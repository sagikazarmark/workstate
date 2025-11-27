from __future__ import annotations

import logging
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

    @overload
    def load(
        self,
        ref: AnyUrl | PurePosixPath,
        dst: DirectoryPath,
        filter: PrefixFilter | None,
    ): ...

    @overload
    def load(
        self,
        ref: AnyUrl | PurePosixPath,
        dst: DirectoryPath,
        filter: PrefixFilter | None = None,
    ): ...

    def load(
        self,
        ref: AnyUrl | PurePosixPath,
        dst: DirectoryPath,
        filter: PrefixFilter | None = None,
    ):
        store, prefix = self._resolve_store_and_path(ref)

        stream: ListStream[Sequence[ObjectMeta]] = obstore.list(
            store,
            prefix=str(prefix) if prefix else None,
        )

        logger = logging.LoggerAdapter(
            self.logger,
            {"ref": str(ref)},
            merge_extra=True,
        )

        logger.info("Loading directory")

        for objects in stream:
            for object in objects:
                object_key = object.path  # Keep original for download
                object_path = PurePosixPath(object_key)
                relative_path = object_path

                # Strip the prefix to get relative path for filtering and local storage
                if prefix is not None:
                    if not object_path.is_relative_to(prefix):
                        # This should never happen :)
                        continue

                    relative_path = object_path.relative_to(prefix)

                # Apply filter to the relative path
                if filter and not filter.match(relative_path):
                    continue

                # Save to local path using relative structure
                dst_file = dst / relative_path
                dst_file.parent.mkdir(parents=True, exist_ok=True)

                logger.info("Downloading file", extra={"key": object_key})

                # Download using the full object key
                data = obstore.get(store, object_key).bytes().to_bytes()
                dst_file.write_bytes(data)

        logger.info("Finished loading directory")


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

    @overload
    def persist(
        self,
        ref: AnyUrl | PurePosixPath,
        src: DirectoryPath,
        filter: PathFilter | None,
    ): ...

    @overload
    def persist(
        self,
        ref: AnyUrl | PurePosixPath,
        src: DirectoryPath,
        filter: PathFilter | None = None,
    ): ...

    def persist(
        self,
        ref: AnyUrl | PurePosixPath,
        src: DirectoryPath,
        filter: PathFilter | None = None,
    ):
        store, prefix = self._resolve_store_and_path(ref)

        logger = logging.LoggerAdapter(
            self.logger,
            {"ref": str(ref), "src": str(src)},
            merge_extra=True,
        )

        logger.info("Starting persistence")

        count = 0

        for file_path in _filter_files(src, filter):
            relative_path = file_path.relative_to(src).as_posix()
            object_key = str(relative_path)

            if prefix is not None:
                object_key = str(prefix.joinpath(relative_path))

            logger.info("Uploading file", extra={"src": str(relative_path)})

            store.put(object_key, file_path)
            count += 1

        if count == 0:
            logger.warning("No files persisted")

        logger.info("Finished persistence", extra={"count": count})
