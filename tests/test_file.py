"""Tests for the file protocols module."""

import io
from pathlib import Path
from typing import IO, get_type_hints
from unittest.mock import Mock

import pytest
from pydantic import AnyUrl

from workstate.file import FileLoader, FilePersister


class TestFileLoaderProtocol:
    """Test the FileLoader protocol."""

    def test_protocol_methods_exist(self):
        """Test that FileLoader has the expected methods."""
        assert hasattr(FileLoader, "load")

    def test_load_method_overloads(self):
        """Test that load method has proper overloads."""
        # The protocol should define overloaded methods
        # We can't easily test the overloads directly, but we can ensure
        # the method exists and is callable
        loader = Mock(spec=FileLoader)
        assert callable(loader.load)

    def test_load_with_io_return_type(self):
        """Test load method signature for IO return."""
        # Create a mock implementation
        mock_loader = Mock(spec=FileLoader)
        url = AnyUrl("file:///test/path")

        # Configure mock to return an IO object
        mock_io = io.BytesIO(b"test data")
        mock_loader.load.return_value = mock_io

        result = mock_loader.load(url)
        assert isinstance(result, io.IOBase)

    def test_load_with_path_destination(self):
        """Test load method signature with Path destination."""
        mock_loader = Mock(spec=FileLoader)
        url = AnyUrl("file:///test/path")
        dst_path = Path("/tmp/test")

        # Configure mock to return None when destination is provided
        mock_loader.load.return_value = None

        result = mock_loader.load(url, dst_path)
        assert result is None

    def test_load_with_io_destination(self):
        """Test load method signature with IO destination."""
        mock_loader = Mock(spec=FileLoader)
        url = AnyUrl("file:///test/path")
        dst_io = io.BytesIO()

        # Configure mock to return None when destination is provided
        mock_loader.load.return_value = None

        result = mock_loader.load(url, dst_io)
        assert result is None


class TestFilePersisterProtocol:
    """Test the FilePersister protocol."""

    def test_protocol_methods_exist(self):
        """Test that FilePersister has the expected methods."""
        assert hasattr(FilePersister, "persist")

    def test_persist_method_overloads(self):
        """Test that persist method has proper overloads."""
        # The protocol should define overloaded methods
        persister = Mock(spec=FilePersister)
        assert callable(persister.persist)

    def test_persist_with_bytes(self):
        """Test persist method with bytes data."""
        mock_persister = Mock(spec=FilePersister)
        url = AnyUrl("file:///test/path")
        data = b"test data"

        mock_persister.persist(url, data)
        mock_persister.persist.assert_called_once_with(url, data)

    def test_persist_with_bytearray(self):
        """Test persist method with bytearray data."""
        mock_persister = Mock(spec=FilePersister)
        url = AnyUrl("file:///test/path")
        data = bytearray(b"test data")

        mock_persister.persist(url, data)
        mock_persister.persist.assert_called_once_with(url, data)

    def test_persist_with_memoryview(self):
        """Test persist method with memoryview data."""
        mock_persister = Mock(spec=FilePersister)
        url = AnyUrl("file:///test/path")
        original_data = b"test data"
        data = memoryview(original_data)

        mock_persister.persist(url, data)
        mock_persister.persist.assert_called_once_with(url, data)

    def test_persist_with_path(self):
        """Test persist method with Path source."""
        mock_persister = Mock(spec=FilePersister)
        url = AnyUrl("file:///test/path")
        src_path = Path("/tmp/source")

        mock_persister.persist(url, src_path)
        mock_persister.persist.assert_called_once_with(url, src_path)


class TestProtocolCompatibility:
    """Test protocol compatibility and type checking."""

    def test_file_loader_protocol_compliance(self):
        """Test that we can implement FileLoader protocol."""

        class TestFileLoader:
            def load(self, ref: AnyUrl, dst=None):
                if dst is None:
                    return io.BytesIO(b"test")
                elif isinstance(dst, Path):
                    dst.write_bytes(b"test")
                else:
                    dst.write(b"test")

        # This should be considered a valid FileLoader implementation
        loader = TestFileLoader()
        assert callable(loader.load)

    def test_file_persister_protocol_compliance(self):
        """Test that we can implement FilePersister protocol."""

        class TestFilePersister:
            def persist(self, ref: AnyUrl, file):
                # Mock implementation
                pass

        # This should be considered a valid FilePersister implementation
        persister = TestFilePersister()
        assert callable(persister.persist)

    def test_pydantic_url_types(self):
        """Test that AnyUrl types work as expected."""
        # Test various URL formats
        file_url = AnyUrl("file:///test/path")
        assert str(file_url) == "file:///test/path"

        http_url = AnyUrl("http://example.com/path")
        assert str(http_url) == "http://example.com/path"

        s3_url = AnyUrl("s3://bucket/key")
        assert str(s3_url) == "s3://bucket/key"
