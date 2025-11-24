"""Tests for the main workstate module."""

import pytest

from workstate import FileLoader, FilePersister, obstore


def test_workstate_imports():
    """Test that all expected items are importable from workstate."""
    # Test that we can import the main classes
    assert FileLoader is not None
    assert FilePersister is not None
    assert obstore is not None


def test_workstate_all_exports():
    """Test that __all__ contains the expected exports."""
    import workstate

    expected_exports = {"obstore", "FileLoader", "FilePersister"}
    actual_exports = set(workstate.__all__)

    assert actual_exports == expected_exports


def test_file_loader_protocol():
    """Test that FileLoader is a proper protocol."""
    from workstate.file import FileLoader

    # Check that it has the expected method
    assert hasattr(FileLoader, "load")

    # Check that it's a protocol class
    assert hasattr(FileLoader, "__protocol__") or hasattr(FileLoader, "_is_protocol")


def test_file_persister_protocol():
    """Test that FilePersister is a proper protocol."""
    from workstate.file import FilePersister

    # Check that it has the expected method
    assert hasattr(FilePersister, "persist")

    # Check that it's a protocol class
    assert hasattr(FilePersister, "__protocol__") or hasattr(
        FilePersister, "_is_protocol"
    )
