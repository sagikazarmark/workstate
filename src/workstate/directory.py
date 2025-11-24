from pathlib import Path, PurePosixPath
from typing import Iterator, Protocol, overload

from pydantic import AnyUrl, DirectoryPath


class Filter(Protocol):
    def match(self, path: str) -> bool: ...


class DirectoryLoader(Protocol):
    @overload
    def load(self, ref: AnyUrl | PurePosixPath, dst: DirectoryPath): ...

    @overload
    def load(self, ref: AnyUrl | PurePosixPath, dst: DirectoryPath, filter: Filter): ...


class DirectoryPersister(Protocol):
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


def _filter_files(path: DirectoryPath, filter: Filter | None) -> Iterator[Path]:
    for file_path in path.rglob("*"):
        if not file_path.is_file():
            continue

        if filter and not filter.match(str(file_path.relative_to(path))):
            continue

        yield file_path
