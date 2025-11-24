import os
from pathlib import Path, PurePosixPath
from typing import overload

import fsspec
from pydantic import AnyUrl

from ..directory import PathFilter, PrefixFilter


class DirectoryLoader:
    def __init__(self, fs: fsspec.AbstractFileSystem | None = None):
        self.fs = fs

    @overload
    def load(self, ref: AnyUrl | PurePosixPath, dst: Path): ...

    @overload
    def load(self, ref: AnyUrl | PurePosixPath, dst: Path, filter: PrefixFilter): ...

    def load(
        self, ref: AnyUrl | PurePosixPath, dst: Path, filter: PrefixFilter | None = None
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
                fs = fsspec.filesystem(ref.scheme, **_extract_fs_options(ref))
            else:
                fs = self.fs

        # Ensure destination directory exists
        dst.mkdir(parents=True, exist_ok=True)

        # Extract the local path from URL for comparison
        if isinstance(ref, AnyUrl) and ref.scheme == "file":
            # For file:// URLs, extract the local path
            local_ref_path = ref.path
        else:
            local_ref_path = ref_str

        # Normalize ref_str to remove trailing slash
        ref_str = ref_str.rstrip("/")
        local_ref_path = local_ref_path.rstrip("/")

        # List all files recursively
        try:
            all_files = fs.find(ref_str, detail=False)
        except (FileNotFoundError, OSError):
            # Directory doesn't exist or is empty
            return

        for file_path in all_files:
            # Convert to relative path from the reference using the local path
            if file_path.startswith(local_ref_path + "/"):
                rel_path = file_path[len(local_ref_path) + 1 :]
            elif file_path == local_ref_path:
                # This is the directory itself, skip
                continue
            else:
                # Handle edge case where path doesn't start with exact match
                if (
                    len(file_path) > len(local_ref_path)
                    and file_path[len(local_ref_path)] == "/"
                ):
                    rel_path = file_path[len(local_ref_path) + 1 :]
                else:
                    # File is at the same level or outside, use basename
                    rel_path = file_path.split("/")[-1]

            # Apply filter if provided
            if filter is not None and not filter.match(rel_path):
                continue

            # Create destination path
            dst_file = dst / rel_path
            dst_file.parent.mkdir(parents=True, exist_ok=True)

            # Download file
            try:
                # Check if it's actually a file (not directory)
                if fs.isfile(file_path):
                    with fs.open(file_path, "rb") as src_file:
                        data = src_file.read()
                        dst_file.write_bytes(data)
            except (IsADirectoryError, PermissionError):
                # Skip directories and permission errors
                continue


class DirectoryPersister:
    def __init__(self, fs: fsspec.AbstractFileSystem | None = None):
        self.fs = fs

    @overload
    def persist(
        self,
        ref: AnyUrl | PurePosixPath,
        src: Path,
    ): ...

    @overload
    def persist(
        self,
        ref: AnyUrl | PurePosixPath,
        src: Path,
        filter: PathFilter,
    ): ...

    def persist(
        self,
        ref: AnyUrl | PurePosixPath,
        src: Path,
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
                fs = fsspec.filesystem(ref.scheme, **_extract_fs_options(ref))
            else:
                fs = self.fs

        if not src.exists():
            raise FileNotFoundError(f"Source directory does not exist: {src}")

        if not src.is_dir():
            raise NotADirectoryError(f"Source is not a directory: {src}")

        # Walk through source directory
        for root, dirs, files in os.walk(src):
            for file_name in files:
                src_file = Path(root) / file_name

                # Get relative path from source root
                rel_path = src_file.relative_to(src)
                rel_path_str = str(rel_path).replace(os.sep, "/")

                # Apply filter if provided
                if filter is not None and not filter.match(rel_path_str):
                    continue

                # Create destination path
                dst_path = f"{ref_str.rstrip('/')}/{rel_path_str}"

                # Ensure parent directory exists
                parent_path = str(Path(dst_path).parent).replace(os.sep, "/")
                if parent_path != ".":
                    try:
                        fs.makedirs(parent_path, exist_ok=True)
                    except (AttributeError, NotImplementedError):
                        # Some filesystems don't support makedirs
                        pass

                # Upload file
                with src_file.open("rb") as src_f:
                    with fs.open(dst_path, "wb") as dst_f:
                        dst_f.write(src_f.read())


def _extract_fs_options(url: AnyUrl) -> dict:
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
