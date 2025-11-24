from __future__ import annotations

import io
from pathlib import Path, PurePosixPath
from typing import IO, TYPE_CHECKING, overload

import obstore
import obstore.store
from pydantic import AnyUrl

if TYPE_CHECKING:
    from obstore.store import ClientConfig


class _FileBase:
    def __init__(
        self,
        store: obstore.store.ObjectStore | None = None,
        client_options: ClientConfig | None = None,
    ):
        self.store = store
        self.client_options = client_options

    def _resolve_store(
        self,
        url: AnyUrl,
    ) -> tuple[obstore.store.ObjectStore, str]:
        path = PurePosixPath(url.path or "/")
        host = url.host

        if host is None:
            # For schemes like file:// where host might be in the path
            if path.parts:
                host = path.parts[0]
                # Reconstruct path from remaining parts
                path = (
                    PurePosixPath(*path.parts[1:])
                    if len(path.parts) > 1
                    else PurePosixPath()
                )
            else:
                host = ""

        store_url = f"{url.scheme}://{host}"

        return obstore.store.from_url(
            store_url,
            client_options=self.client_options,
        ), str(path)


class FileLoader(_FileBase):
    @overload
    def load(self, ref: AnyUrl | PurePosixPath) -> IO: ...

    @overload
    def load(self, ref: AnyUrl | PurePosixPath, dst: Path): ...

    @overload
    def load(self, ref: AnyUrl | PurePosixPath, dst: IO): ...

    def load(
        self, ref: AnyUrl | PurePosixPath, dst: Path | IO | None = None
    ) -> IO | None:
        store = self.store

        # Handle path vs URL references
        if isinstance(ref, PurePosixPath):
            if store is None:
                raise ValueError(
                    "Cannot use path reference without a configured store. "
                    "Either provide a URL reference or configure a store in the constructor."
                )
            path = str(ref)
        else:
            # ref is AnyUrl
            path = str(ref)
            if store is None:
                store, path = self._resolve_store(ref)

        # TODO: https://github.com/developmentseed/obstore/pull/593
        # TODO: https://github.com/developmentseed/obstore/issues/314
        data = obstore.get(store, path).bytes().to_bytes()

        if dst is None:
            # Return as IO
            return io.BytesIO(data)
        elif isinstance(dst, Path):
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
        self, ref: AnyUrl | PurePosixPath, src: bytes | bytearray | memoryview
    ): ...

    @overload
    def persist(self, ref: AnyUrl | PurePosixPath, src: Path): ...

    def persist(
        self, ref: AnyUrl | PurePosixPath, src: bytes | bytearray | memoryview | Path
    ):
        store = self.store

        # Handle path vs URL references
        if isinstance(ref, PurePosixPath):
            if store is None:
                raise ValueError(
                    "Cannot use path reference without a configured store. "
                    "Either provide a URL reference or configure a store in the constructor."
                )
            path = str(ref)
        else:
            # ref is AnyUrl
            path = str(ref)
            if store is None:
                store, path = self._resolve_store(ref)

        obstore.put(store, path, src)
