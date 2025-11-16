from collections.abc import Iterator
from contextlib import contextmanager
from typing import Protocol, TypeVar

Options = TypeVar("Options", contravariant=True)


class StateManager(Protocol[Options]):
    @contextmanager
    def save(self, options: Options) -> Iterator[str]: ...

    # @contextmanager
    # def load(self, options: Options) -> Iterator[str]: ...
