import io
from pathlib import Path, PurePosixPath
from typing import IO, cast, overload

import fsspec
from pydantic import AnyUrl


class FileLoader:
    def __init__(self, fs: fsspec.AbstractFileSystem | None = None):
        self.fs = fs

    @overload
    def load(self, ref: AnyUrl | PurePosixPath) -> IO: ...

    @overload
    def load(self, ref: AnyUrl | PurePosixPath, dst: Path): ...

    @overload
    def load(self, ref: AnyUrl | PurePosixPath, dst: IO): ...

    def load(
        self, ref: AnyUrl | PurePosixPath, dst: Path | IO | None = None
    ) -> IO | None:
        # Handle path vs URL references
        if isinstance(ref, PurePosixPath):
            if self.fs is None:
                raise ValueError(
                    "Cannot use path reference without a configured filesystem. "
                    "Either provide a URL reference or configure a filesystem in the constructor."
                )
            ref_str = str(ref)
        else:
            # ref is AnyUrl
            ref_str = str(ref)

        if dst is None:
            # Return as IO - ensure binary mode and cast to IO
            if self.fs is None:
                # ref_str is from URL, so this always returns OpenFile
                return cast(
                    IO, cast(fsspec.core.OpenFile, fsspec.open(ref_str, "rb")).open()
                )

            return cast(IO, self.fs.open(ref_str, "rb"))
        else:
            # Read the data - ensure binary mode
            if self.fs is None:
                with cast(fsspec.core.OpenFile, fsspec.open(ref_str, "rb")).open() as f:
                    data = f.read()
            else:
                with self.fs.open(ref_str, "rb") as f:
                    data = f.read()

            # Ensure data is bytes
            if isinstance(data, str):
                data = data.encode("utf-8")

            if isinstance(dst, Path):
                # Write to the provided path
                dst.write_bytes(data)
            else:
                # Write to the provided IO object
                dst.write(data)

            return None


class FilePersister:
    def __init__(self, fs: fsspec.AbstractFileSystem | None = None):
        self.fs = fs

    @overload
    def persist(
        self, ref: AnyUrl | PurePosixPath, src: bytes | bytearray | memoryview
    ): ...

    @overload
    def persist(self, ref: AnyUrl | PurePosixPath, src: Path): ...

    def persist(
        self, ref: AnyUrl | PurePosixPath, src: bytes | bytearray | memoryview | Path
    ):
        # Handle path vs URL references
        if isinstance(ref, PurePosixPath):
            if self.fs is None:
                raise ValueError(
                    "Cannot use path reference without a configured filesystem. "
                    "Either provide a URL reference or configure a filesystem in the constructor."
                )
            ref_str = str(ref)
        else:
            # ref is AnyUrl
            ref_str = str(ref)

        if self.fs is None:
            # ref_str is from URL, so this always returns OpenFile
            of = cast(
                fsspec.core.OpenFile,
                fsspec.open(ref_str, "wb"),
            )
        else:
            of = self.fs.open(ref_str, "wb")

        with of as f:
            if isinstance(src, Path):
                # Read from source path and write
                data = src.read_bytes()
                cast(io.IOBase, f).write(data)
            else:
                # src is bytes, bytearray, or memoryview
                cast(io.IOBase, f).write(src)
