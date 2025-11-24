from pathlib import Path, PurePosixPath
from typing import IO, Protocol, overload

from pydantic import AnyUrl, FilePath


class FileLoader(Protocol):
    @overload
    def load(self, ref: AnyUrl | PurePosixPath) -> IO: ...

    @overload
    def load(self, ref: AnyUrl | PurePosixPath, dst: Path): ...

    @overload
    def load(self, ref: AnyUrl | PurePosixPath, dst: IO): ...


class FilePersister(Protocol):
    @overload
    def persist(
        self,
        ref: AnyUrl | PurePosixPath,
        src: bytes | bytearray | memoryview,
    ): ...

    @overload
    def persist(self, ref: AnyUrl | PurePosixPath, src: FilePath): ...
