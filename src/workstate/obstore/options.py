from pathlib import PurePosixPath
from typing import Annotated, Protocol, runtime_checkable

from pydantic import AfterValidator, AnyUrl

from .filter import Filter


def validate_no_parent_refs(path: PurePosixPath) -> PurePosixPath:
    if ".." in path.parts:
        raise ValueError('Prefix cannot contain ".." components')

    return path


Prefix = Annotated[PurePosixPath, AfterValidator(validate_no_parent_refs)]


@runtime_checkable
class HasUrl(Protocol):
    url: AnyUrl


@runtime_checkable
class MayHaveUrl(Protocol):
    url: AnyUrl | None


@runtime_checkable
class HasPrefix(Protocol):
    prefix: Prefix


@runtime_checkable
class MayHavePrefix(Protocol):
    prefix: Prefix | None


@runtime_checkable
class HasFilter(Protocol):
    filter: Filter


@runtime_checkable
class MayHaveFilter(Protocol):
    filter: Filter | None


def resolve_prefix(options: object) -> Prefix:
    match options:
        case HasPrefix():
            return options.prefix

        case MayHavePrefix():
            return options.prefix or PurePosixPath("")

        case _:
            return PurePosixPath("")


def resolve_filter(options: object) -> Filter | None:
    match options:
        case HasFilter():
            return options.filter

        case MayHaveFilter():
            return options.filter

        case _:
            return None
