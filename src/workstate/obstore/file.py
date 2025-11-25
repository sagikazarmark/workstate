import io
from pathlib import Path, PurePosixPath
from typing import IO, overload

import obstore
import obstore.store
from pydantic import AnyUrl, FilePath

from .base import _Base


class _FileBase(_Base):
    pass


class FileLoader(_FileBase):
    @overload
    def load(self, ref: AnyUrl | PurePosixPath) -> IO: ...

    @overload
    def load(self, ref: AnyUrl | PurePosixPath, dst: Path): ...

    @overload
    def load(self, ref: AnyUrl | PurePosixPath, dst: IO): ...

    def load(
        self,
        ref: AnyUrl | PurePosixPath,
        dst: Path | IO | None = None,
    ) -> IO | None:
        store, path = self._resolve_store_and_path(ref)

        if path is None:
            raise ValueError("Cannot load file with empty path")

        # TODO: https://github.com/developmentseed/obstore/pull/593
        # TODO: https://github.com/developmentseed/obstore/issues/314
        data = obstore.get(store, str(path)).bytes().to_bytes()

        if dst is None:  # Return as IO
            return io.BytesIO(data)
        elif isinstance(dst, Path):  # Write to the provided path
            dst.write_bytes(data)

            return None
        else:  # Write to the provided IO object
            dst.write(data)

            return None


class FilePersister(_FileBase):
    @overload
    def persist(
        self,
        ref: AnyUrl | PurePosixPath,
        src: bytes | bytearray | memoryview,
    ): ...

    @overload
    def persist(self, ref: AnyUrl | PurePosixPath, src: FilePath): ...

    def persist(
        self,
        ref: AnyUrl | PurePosixPath,
        src: bytes | bytearray | memoryview | FilePath,
    ):
        store, path = self._resolve_store_and_path(ref)

        if path is None:
            raise ValueError("Cannot persist file with empty path")

        obstore.put(store, str(path), src)
