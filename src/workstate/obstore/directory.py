from __future__ import annotations

from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Sequence, overload

import obstore
from pydantic import AnyUrl, DirectoryPath

from ..directory import Filter, _filter_files
from .base import _Base

if TYPE_CHECKING:
    from obstore import ListStream, ObjectMeta


class _DirectoryBase(_Base):
    pass


class DirectoryLoader(_DirectoryBase):
    @overload
    def load(self, ref: AnyUrl | PurePosixPath, dst: DirectoryPath): ...

    @overload
    def load(self, ref: AnyUrl | PurePosixPath, dst: DirectoryPath, filter: Filter): ...

    def load(
        self,
        ref: AnyUrl | PurePosixPath,
        dst: DirectoryPath,
        filter: Filter | None = None,
    ):
        store, path = self._resolve_store_and_path(ref)

        stream: ListStream[Sequence[ObjectMeta]] = obstore.list(
            store,
            prefix=str(path) or None,
        )

        for objects in stream:
            for obj in objects:
                obj_path = PurePosixPath(obj.path)

                if obj_path.is_relative_to(path):
                    obj_path = obj_path.relative_to(path)

                if filter and not filter.match(str(obj_path)):
                    continue

                dst_file = dst / obj_path
                dst_file.parent.mkdir(parents=True, exist_ok=True)

                data = obstore.get(store, str(obj_path)).bytes().to_bytes()
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
        filter: Filter,
    ): ...

    def persist(
        self,
        ref: AnyUrl | PurePosixPath,
        src: DirectoryPath,
        filter: Filter | None = None,
    ):
        store, path = self._resolve_store_and_path(ref)

        for file_path in _filter_files(src, filter):
            relative_path = file_path.relative_to(src)
            object_key = str(path.joinpath(relative_path))

            # self.logger.info("Uploading file", extra={"file": str(relative_path)})

            store.put(object_key, file_path)
