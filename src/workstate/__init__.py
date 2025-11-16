from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Protocol, TypeVar

from . import obstore

Options = TypeVar("Options", contravariant=True, default=Any)


class StateManager(Protocol[Options]):
    @contextmanager
    def save(self, options: Options) -> Iterator[str]: ...

    # @contextmanager
    # def load(self, options: Options) -> Iterator[str]: ...


__all__ = [
    "obstore",
    "Options",
    "StateManager",
]
