import io
from pathlib import Path, PurePosixPath
from typing import IO, cast, overload

import fsspec
import fsspec.core
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
        # Handle path vs URL references using base class logic
        if isinstance(ref, PurePosixPath):
            if self.fs is None:
                raise ValueError(
                    "Cannot use path reference without a configured filesystem. "
                    "Either provide a URL reference or configure a filesystem in the constructor."
                )
            ref_str = str(ref)
            fs = self.fs
        else:
            # ref is AnyUrl
            ref_str = str(ref)
            if self.fs is not None:
                # Use configured filesystem
                fs = self.fs
            else:
                # No configured filesystem - use fsspec.open for backward compatibility
                fs = None

        if dst is None:
            # Return as IO - ensure binary mode and cast to IO
            if fs is None:
                # ref_str is from URL, so this always returns OpenFile
                return cast(
                    IO, cast(fsspec.core.OpenFile, fsspec.open(ref_str, "rb")).open()
                )

            return cast(IO, fs.open(ref_str, "rb"))
        else:
            # Read the data - ensure binary mode
            if fs is None:
                with cast(fsspec.core.OpenFile, fsspec.open(ref_str, "rb")).open() as f:
                    data = f.read()
            else:
                with fs.open(ref_str, "rb") as f:
                    data = f.read()

            # Ensure data is bytes
            if isinstance(data, str):
                data = data.encode("utf-8")

            if isinstance(dst, Path):
                # Write to the provided path
                dst.write_bytes(data)
                return None
            else:
                # Write to the provided IO object
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
        # Handle path vs URL references using base class logic
        if isinstance(ref, PurePosixPath):
            if self.fs is None:
                raise ValueError(
                    "Cannot use path reference without a configured filesystem. "
                    "Either provide a URL reference or configure a filesystem in the constructor."
                )
            ref_str = str(ref)
            fs = self.fs
        else:
            # ref is AnyUrl
            ref_str = str(ref)
            fs = self.fs

        if fs is None:
            # ref_str is from URL, so this always returns OpenFile
            of = cast(
                fsspec.core.OpenFile,
                fsspec.open(ref_str, "wb"),
            )
        else:
            of = fs.open(ref_str, "wb")

        with of as f:
            if isinstance(src, Path):
                # Read from source path and write
                data = src.read_bytes()
                cast(io.IOBase, f).write(data)
            else:
                # src is bytes, bytearray, or memoryview
                cast(io.IOBase, f).write(src)
