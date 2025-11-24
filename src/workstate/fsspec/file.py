import io
from pathlib import Path
from typing import IO, cast, overload

import fsspec
from pydantic import AnyUrl


class FileLoader:
    def __init__(self, fs: fsspec.AbstractFileSystem | None = None):
        self.fs = fs

    @overload
    def load(self, ref: AnyUrl) -> IO: ...

    @overload
    def load(self, ref: AnyUrl, dst: Path) -> None: ...

    @overload
    def load(self, ref: AnyUrl, dst: IO) -> None: ...

    def load(self, ref: AnyUrl, dst: Path | IO | None = None) -> IO | None:
        if dst is None:
            # Return as IO - ensure binary mode and cast to IO
            if self.fs is None:
                # ref is string, so this always returns OpenFile
                return cast(
                    IO, cast(fsspec.core.OpenFile, fsspec.open(str(ref), "rb")).open()
                )

            return cast(IO, self.fs.open(str(ref), "rb"))
        else:
            # Read the data - ensure binary mode
            if self.fs is None:
                with cast(
                    fsspec.core.OpenFile, fsspec.open(str(ref), "rb")
                ).open() as f:
                    data = f.read()
            else:
                with self.fs.open(str(ref), "rb") as f:
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
    def persist(self, ref: AnyUrl, src: bytes | bytearray | memoryview): ...

    @overload
    def persist(self, ref: AnyUrl, src: Path): ...

    def persist(self, ref: AnyUrl, src: bytes | bytearray | memoryview | Path):
        if self.fs is None:
            # ref is string, so this always returns OpenFile
            of = cast(
                fsspec.core.OpenFile,
                fsspec.open(ref, "wb"),
            )
        else:
            of = self.fs.open(ref, "wb")

        with of as f:
            cast(io.IOBase, f).write(src)
