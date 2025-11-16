from collections.abc import Iterator
from fnmatch import fnmatch
from pathlib import Path
from typing import Protocol

from pydantic import BaseModel, Field


class Filter(Protocol):
    def match(self, path: str) -> bool: ...


def filter_files(path: Path, filter: Filter | None) -> Iterator[Path]:
    for file_path in path.rglob("*"):
        if not file_path.is_file():
            continue

        if not filter or filter.match(str(file_path.relative_to(path))):
            yield file_path


class IncludeExcludeFilter(BaseModel):
    """Filter for which files to upload using glob patterns."""

    include: list[str] = Field(
        default=[],
        description="List of glob patterns to include",
    )
    exclude: list[str] = Field(
        default=[],
        description="List of glob patterns to exclude",
    )

    def match(self, path: str) -> bool:
        """
        Check if a file should be uploaded based on the filter.

        Args:
            path: The relative path of the file.

        Returns:
            True if the file should be uploaded, False otherwise.
        """

        # Check exclude patterns first
        if self.exclude:
            if any(fnmatch(path, pattern) for pattern in self.exclude):
                return False

        # If include patterns exist, path must match at least one
        if self.include:
            if not any(fnmatch(path, pattern) for pattern in self.include):
                return False

        return True
