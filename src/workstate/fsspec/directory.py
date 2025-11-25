from __future__ import annotations

import io
from pathlib import PurePosixPath
from typing import cast, overload

import fsspec
from pydantic import AnyUrl, DirectoryPath

from ..directory import PathFilter, PrefixFilter, _filter_files
from .base import _Base


class _DirectoryBase(_Base):
    pass


class DirectoryLoader(_DirectoryBase):
    @overload
    def load(self, ref: AnyUrl | PurePosixPath, dst: DirectoryPath): ...

    @overload
    def load(
        self,
        ref: AnyUrl | PurePosixPath,
        dst: DirectoryPath,
        filter: PrefixFilter,
    ): ...

    def load(
        self,
        ref: AnyUrl | PurePosixPath,
        dst: DirectoryPath,
        filter: PrefixFilter | None = None,
    ):
        # Handle path vs URL references
        if isinstance(ref, PurePosixPath):
            if self.fs is None:
                raise ValueError(
                    "Cannot use path reference without a configured filesystem. "
                    "Either provide a URL reference or configure a filesystem in the constructor."
                )
            ref_str = str(ref)
            fs = self.fs
        else:
            # ref is AnyUrl
            ref_str = str(ref)
            if self.fs is None:
                # Create filesystem from URL
                fs = fsspec.filesystem(ref.scheme, **self._extract_fs_options(ref))
            else:
                fs = self.fs

        # Ensure destination directory exists
        dst.mkdir(parents=True, exist_ok=True)

        # For configured filesystem, preserve original URL format
        if self.fs is not None:
            find_path = ref_str
        else:
            # For created filesystem, normalize path
            find_path = ref_str.rstrip("/")

        # List all files recursively
        try:
            if self.fs is not None:
                # Configured filesystem - use simpler call format
                all_files = fs.find(find_path)
            else:
                # Created filesystem - use detail=False
                all_files = fs.find(find_path, detail=False)
        except (FileNotFoundError, OSError):
            # Directory doesn't exist or is empty
            return

        for file_path in all_files:
            # Convert to PurePosixPath for consistent handling
            file_posix_path = PurePosixPath(file_path)

            # Calculate relative path
            if ref_str and not isinstance(ref, PurePosixPath):
                # For URL references, extract the path portion for comparison
                url_path = ref.path or "" if isinstance(ref, AnyUrl) else ""
                url_path = url_path.rstrip("/")

                if url_path:
                    # Handle both absolute paths (file://) and relative paths (s3://, etc.)
                    # Try with leading slash first (for file:// URLs)
                    if file_path.startswith(url_path + "/"):
                        rel_path_str = file_path[len(url_path) + 1 :]
                    elif file_path == url_path:
                        # This is the directory itself, skip
                        continue
                    else:
                        # Try without leading slash (for s3://, gcs://, etc.)
                        url_path_no_slash = url_path.lstrip("/")
                        if url_path_no_slash and file_path.startswith(
                            url_path_no_slash + "/"
                        ):
                            rel_path_str = file_path[len(url_path_no_slash) + 1 :]
                        elif file_path == url_path_no_slash:
                            # This is the directory itself, skip
                            continue
                        else:
                            # Use the full file path as relative
                            rel_path_str = file_path
                else:
                    # No path in URL, use full file path
                    rel_path_str = file_path

                relative_path = PurePosixPath(rel_path_str)
            elif isinstance(ref, PurePosixPath):
                # For path references, calculate relative to the path
                ref_path_str = str(ref)
                if file_path.startswith(ref_path_str + "/"):
                    rel_path_str = file_path[len(ref_path_str) + 1 :]
                elif file_path == ref_path_str:
                    # This is the directory itself, skip
                    continue
                else:
                    # Use just the filename
                    rel_path_str = PurePosixPath(file_path).name
                relative_path = PurePosixPath(rel_path_str)
            else:
                # No reference path, use the full file path as relative
                relative_path = file_posix_path

            # Apply filter to the relative path
            if filter and not filter.match(relative_path):
                continue

            # Skip if this is a directory (we only want files)
            try:
                if not fs.isfile(file_path):
                    continue
            except (IsADirectoryError, PermissionError, OSError):
                continue

            # Save to local path using relative structure
            dst_file = dst / relative_path
            dst_file.parent.mkdir(parents=True, exist_ok=True)

            # Download file
            try:
                with fs.open(file_path, "rb") as src_f:
                    data = src_f.read()
                    # Ensure data is bytes
                    if isinstance(data, str):
                        data = data.encode("utf-8")
                    dst_file.write_bytes(data)
            except (IsADirectoryError, PermissionError, OSError):
                # Skip files we can't read
                continue


class DirectoryPersister(_DirectoryBase):
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

    def persist(
        self,
        ref: AnyUrl | PurePosixPath,
        src: DirectoryPath,
        filter: PathFilter | None = None,
    ):
        # Handle path vs URL references
        if isinstance(ref, PurePosixPath):
            if self.fs is None:
                raise ValueError(
                    "Cannot use path reference without a configured filesystem. "
                    "Either provide a URL reference or configure a filesystem in the constructor."
                )
            ref_str = str(ref)
            fs = self.fs
        else:
            # ref is AnyUrl
            ref_str = str(ref)
            if self.fs is None:
                # Create filesystem from URL
                fs = fsspec.filesystem(ref.scheme, **self._extract_fs_options(ref))
            else:
                fs = self.fs

        # Use the shared filter logic from the common directory module
        for file_path in _filter_files(src, filter):
            relative_path = file_path.relative_to(src)

            # Build the destination path
            if ref_str and ref_str not in ("", ".", "/"):
                # For URLs, we need to handle the path portion correctly
                if isinstance(ref, AnyUrl):
                    url_path = ref.path or ""
                    if url_path and url_path not in ("", ".", "/"):
                        dest_path = str(PurePosixPath(url_path).joinpath(relative_path))
                    else:
                        dest_path = str(relative_path)
                else:
                    # For path references
                    dest_path = str(PurePosixPath(ref_str).joinpath(relative_path))
            else:
                # Use relative path directly
                dest_path = str(relative_path)

            # Ensure parent directories exist (if filesystem supports it)
            parent_path = str(PurePosixPath(dest_path).parent)
            if parent_path and parent_path != ".":
                try:
                    fs.makedirs(parent_path, exist_ok=True)
                except (AttributeError, NotImplementedError, OSError):
                    # Some filesystems don't support makedirs or it might fail
                    pass

            # Upload file
            try:
                with file_path.open("rb") as src_f:
                    data = src_f.read()
                    # Ensure we have bytes for binary writing
                    if isinstance(data, str):
                        data = data.encode("utf-8")
                    with fs.open(dest_path, "wb") as dst_f:
                        cast(io.IOBase, dst_f).write(data)
            except (PermissionError, OSError) as e:
                # Re-raise for critical errors, but we could also log and continue
                raise e
