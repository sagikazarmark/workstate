from __future__ import annotations

from pathlib import PurePosixPath
from typing import TYPE_CHECKING

import obstore
import obstore.store
from pydantic import AnyUrl

if TYPE_CHECKING:
    from obstore.store import ClientConfig


class _Base:
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

    def _resolve_store_and_path(
        self,
        ref: AnyUrl | PurePosixPath,
    ) -> tuple[obstore.store.ObjectStore, PurePosixPath]:
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

        return store, PurePosixPath(path)
