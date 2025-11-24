from pathlib import Path, PurePath, PurePosixPath
from typing import Iterator, Protocol, overload

from pydantic import AnyUrl, DirectoryPath


class PrefixFilter(Protocol):
    def match(self, path: PurePosixPath) -> bool: ...


class DirectoryLoader(Protocol):
    @overload
    def load(self, ref: AnyUrl | PurePosixPath, dst: DirectoryPath): ...

    @overload
    def load(
        self,
        ref: AnyUrl | PurePosixPath,
        dst: DirectoryPath,
        filter: PrefixFilter,
    ): ...


class PathFilter(Protocol):
    def match(self, path: PurePath) -> bool: ...


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
        filter: PathFilter,
    ): ...


def _filter_files(path: DirectoryPath, filter: PathFilter | None) -> Iterator[Path]:
    for file_path in path.rglob("*"):
        if not file_path.is_file():
            continue

        if filter and not filter.match(file_path.relative_to(path)):
            continue

        yield file_path
