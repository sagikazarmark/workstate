from __future__ import annotations

from pathlib import PurePosixPath
from typing import TYPE_CHECKING

import fsspec
from pydantic import AnyUrl

if TYPE_CHECKING:
    pass


class _Base:
    def __init__(
        self,
        fs: fsspec.AbstractFileSystem | None = None,
    ):
        self.fs = fs

    def _resolve_filesystem(
        self,
        url: AnyUrl,
    ) -> tuple[fsspec.AbstractFileSystem, str]:
        # Extract filesystem options from URL
        fs_options = self._extract_fs_options(url)

        # Create filesystem from URL scheme
        fs = fsspec.filesystem(url.scheme, **fs_options)

        # Return the full URL as the path for fsspec
        return fs, str(url)

    def _resolve_filesystem_and_path(
        self,
        ref: AnyUrl | PurePosixPath,
    ) -> tuple[fsspec.AbstractFileSystem, str]:
        # Case 1: Path reference - requires configured filesystem
        if isinstance(ref, PurePosixPath):
            if self.fs is None:
                raise ValueError(
                    "Cannot use path reference without a configured filesystem. "
                    "Either provide a URL reference or configure a filesystem in the constructor."
                )

            return self.fs, str(ref)

        # Case 2: URL reference with configured filesystem - use full URL
        if self.fs is not None:
            return self.fs, str(ref)

        # Case 3: URL reference without configured filesystem - resolve filesystem from URL
        fs, path = self._resolve_filesystem(ref)

        return fs, path

    def _extract_fs_options(self, url: AnyUrl) -> dict:
        """Extract filesystem options from URL."""
        options = {}

        if url.host:
            # For schemes that use host (like s3, gcs, etc.)
            if url.scheme in ("s3", "gs", "gcs"):
                # These typically don't need host in options
                pass
            else:
                options["host"] = url.host

        if url.port:
            options["port"] = url.port

        if url.username:
            options["username"] = url.username

        if url.password:
            options["password"] = url.password

        return options
