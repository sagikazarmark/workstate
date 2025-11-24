"""Tests for the obstore module."""

import pytest

from workstate.obstore import FileLoader, FilePersister


def test_obstore_imports():
    """Test that all expected items are importable from workstate.obstore."""
    # Test that we can import the main classes
    assert FileLoader is not None
    assert FilePersister is not None


def test_obstore_all_exports():
    """Test that __all__ contains the expected exports."""
    from workstate import obstore

    expected_exports = {"FileLoader", "FilePersister"}
    actual_exports = set(obstore.__all__)

    assert actual_exports == expected_exports


def test_obstore_classes_are_different_from_protocols():
    """Test that obstore implementations are different from the protocol definitions."""
    from workstate.file import FileLoader as ProtocolFileLoader
    from workstate.file import FilePersister as ProtocolFilePersister
    from workstate.obstore import FileLoader as ObstoreFileLoader
    from workstate.obstore import FilePersister as ObstoreFilePersister

    # The obstore implementations should be different classes than the protocols
    assert ObstoreFileLoader is not ProtocolFileLoader
    assert ObstoreFilePersister is not ProtocolFilePersister


def test_obstore_implementations_exist():
    """Test that the obstore implementations have the expected structure."""
    from workstate.obstore.file import FileLoader, FilePersister

    # Both should be classes (not protocols)
    assert isinstance(FileLoader, type)
    assert isinstance(FilePersister, type)

    # Both should have the expected methods
    assert hasattr(FileLoader, "load")
    assert hasattr(FilePersister, "persist")

    # Both should be instantiable
    loader = FileLoader()
    persister = FilePersister()

    assert loader is not None
    assert persister is not None
