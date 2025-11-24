from pathlib import Path
from typing import IO, Protocol, overload

from pydantic import AnyUrl


class FileLoader(Protocol):
    @overload
    def load(self, ref: AnyUrl) -> IO: ...

    @overload
    def load(self, ref: AnyUrl, dst: Path): ...

    @overload
    def load(self, ref: AnyUrl, dst: IO): ...


class FilePersister(Protocol):
    @overload
    def persist(self, ref: AnyUrl, data: bytes | bytearray | memoryview): ...

    @overload
    def persist(self, ref: AnyUrl, src: Path): ...
