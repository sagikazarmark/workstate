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
    ) -> tuple[obstore.store.ObjectStore, PurePosixPath | None]:
        path = PurePosixPath(url.path or "")
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

        # Return None for empty or "." paths
        if str(path) in ("", ".", "/"):
            prefix = None
        else:
            prefix = path

        return obstore.store.from_url(
            store_url,
            client_options=self.client_options,
        ), prefix

    def _normalize_path(self, prefix: PurePosixPath | None) -> PurePosixPath | None:
        if prefix is None or str(prefix) in ("", ".", "/"):
            return None

        return prefix

    def _resolve_store_and_path(
        self,
        ref: AnyUrl | PurePosixPath,
    ) -> tuple[obstore.store.ObjectStore, PurePosixPath | None]:
        # Case 1: Path reference - requires configured store
        if isinstance(ref, PurePosixPath):
            if self.store is None:
                raise ValueError(
                    "Cannot use path reference without a configured store. "
                    "Either provide a URL reference or configure a store in the constructor."
                )

            return self.store, self._normalize_path(ref)

        # Case 2: URL reference with configured store - extract path from URL
        if self.store is not None:
            path = PurePosixPath(ref.path or "")
            return self.store, self._normalize_path(path)

        # Case 3: URL reference without configured store - resolve store from URL
        store, path = self._resolve_store(ref)

        return store, self._normalize_path(path)
