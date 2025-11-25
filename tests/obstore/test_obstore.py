"""Tests for the obstore module."""

import pytest

from workstate.obstore import (
    DirectoryLoader,
    DirectoryPersister,
    FileLoader,
    FilePersister,
)


def test_obstore_imports():
    """Test that all expected items are importable from workstate.obstore."""
    # Test that we can import the main classes
    assert DirectoryLoader is not None
    assert DirectoryPersister is not None
    assert FileLoader is not None
    assert FilePersister is not None


def test_obstore_all_exports():
    """Test that __all__ contains the expected exports."""
    from workstate import obstore

    expected_exports = {
        "DirectoryLoader",
        "DirectoryPersister",
        "FileLoader",
        "FilePersister",
    }
    actual_exports = set(obstore.__all__)

    assert actual_exports == expected_exports


def test_obstore_classes_are_different_from_protocols():
    """Test that obstore implementations are different from the protocol definitions."""
    from workstate.directory import DirectoryLoader as ProtocolDirectoryLoader
    from workstate.directory import DirectoryPersister as ProtocolDirectoryPersister
    from workstate.file import FileLoader as ProtocolFileLoader
    from workstate.file import FilePersister as ProtocolFilePersister
    from workstate.obstore import DirectoryLoader as ObstoreDirectoryLoader
    from workstate.obstore import DirectoryPersister as ObstoreDirectoryPersister
    from workstate.obstore import FileLoader as ObstoreFileLoader
    from workstate.obstore import FilePersister as ObstoreFilePersister

    # The obstore implementations should be different classes than the protocols
    assert ObstoreDirectoryLoader is not ProtocolDirectoryLoader
    assert ObstoreDirectoryPersister is not ProtocolDirectoryPersister
    assert ObstoreFileLoader is not ProtocolFileLoader
    assert ObstoreFilePersister is not ProtocolFilePersister


def test_obstore_implementations_exist():
    """Test that the obstore implementations have the expected structure."""
    from workstate.obstore.directory import DirectoryLoader, DirectoryPersister
    from workstate.obstore.file import FileLoader, FilePersister

    # All should be classes (not protocols)
    assert isinstance(DirectoryLoader, type)
    assert isinstance(DirectoryPersister, type)
    assert isinstance(FileLoader, type)
    assert isinstance(FilePersister, type)

    # All should have the expected methods
    assert hasattr(DirectoryLoader, "load")
    assert hasattr(DirectoryPersister, "persist")
    assert hasattr(FileLoader, "load")
    assert hasattr(FilePersister, "persist")

    # All should be instantiable
    dir_loader = DirectoryLoader()
    dir_persister = DirectoryPersister()
    loader = FileLoader()
    persister = FilePersister()

    assert dir_loader is not None
    assert dir_persister is not None
    assert loader is not None
    assert persister is not None
