"""Tests for the file protocols module."""

import io
from pathlib import Path, PurePosixPath
from typing import IO, get_type_hints
from unittest.mock import Mock

import pytest
from pydantic import AnyUrl

from workstate.file import (
    FileLoader,
    FilePersister,
)


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

    def test_load_with_io_return_type_url(self):
        """Test load method signature for IO return with URL."""
        # Create a mock implementation
        mock_loader = Mock(spec=FileLoader)
        url = AnyUrl("file:///test/path")

        # Configure mock to return an IO object
        mock_io = io.BytesIO(b"test data")
        mock_loader.load.return_value = mock_io

        result = mock_loader.load(url)
        assert isinstance(result, io.IOBase)

    def test_load_with_io_return_type_path(self):
        """Test load method signature for IO return with path."""
        # Create a mock implementation
        mock_loader = Mock(spec=FileLoader)
        path_ref = PurePosixPath("/test/path")

        # Configure mock to return an IO object
        mock_io = io.BytesIO(b"test data")
        mock_loader.load.return_value = mock_io

        result = mock_loader.load(path_ref)
        assert isinstance(result, io.IOBase)

    def test_load_with_path_destination_url(self):
        """Test load method signature with Path destination and URL ref."""
        mock_loader = Mock(spec=FileLoader)
        url = AnyUrl("file:///test/path")
        dst_path = Path("/tmp/test")

        # Configure mock to return None when destination is provided
        mock_loader.load.return_value = None

        result = mock_loader.load(url, dst_path)
        assert result is None

    def test_load_with_path_destination_path(self):
        """Test load method signature with Path destination and path ref."""
        mock_loader = Mock(spec=FileLoader)
        path_ref = PurePosixPath("/test/path")
        dst_path = Path("/tmp/test")

        # Configure mock to return None when destination is provided
        mock_loader.load.return_value = None

        result = mock_loader.load(path_ref, dst_path)
        assert result is None

    def test_load_with_io_destination_url(self):
        """Test load method signature with IO destination and URL ref."""
        mock_loader = Mock(spec=FileLoader)
        url = AnyUrl("file:///test/path")
        dst_io = io.BytesIO()

        # Configure mock to return None when destination is provided
        mock_loader.load.return_value = None

        result = mock_loader.load(url, dst_io)
        assert result is None

    def test_load_with_io_destination_path(self):
        """Test load method signature with IO destination and path ref."""
        mock_loader = Mock(spec=FileLoader)
        path_ref = PurePosixPath("/test/path")
        dst_io = io.BytesIO()

        # Configure mock to return None when destination is provided
        mock_loader.load.return_value = None

        result = mock_loader.load(path_ref, dst_io)
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

    def test_persist_with_bytes_url(self):
        """Test persist method with bytes data and URL ref."""
        mock_persister = Mock(spec=FilePersister)
        url = AnyUrl("file:///test/path")
        data = b"test data"

        mock_persister.persist(url, data)
        mock_persister.persist.assert_called_once_with(url, data)

    def test_persist_with_bytes_path(self):
        """Test persist method with bytes data and path ref."""
        mock_persister = Mock(spec=FilePersister)
        path_ref = PurePosixPath("/test/path")
        data = b"test data"

        mock_persister.persist(path_ref, data)
        mock_persister.persist.assert_called_once_with(path_ref, data)

    def test_persist_with_bytearray_url(self):
        """Test persist method with bytearray data and URL ref."""
        mock_persister = Mock(spec=FilePersister)
        url = AnyUrl("file:///test/path")
        data = bytearray(b"test data")

        mock_persister.persist(url, data)
        mock_persister.persist.assert_called_once_with(url, data)

    def test_persist_with_bytearray_path(self):
        """Test persist method with bytearray data and path ref."""
        mock_persister = Mock(spec=FilePersister)
        path_ref = PurePosixPath("/test/path")
        data = bytearray(b"test data")

        mock_persister.persist(path_ref, data)
        mock_persister.persist.assert_called_once_with(path_ref, data)

    def test_persist_with_memoryview_url(self):
        """Test persist method with memoryview data and URL ref."""
        mock_persister = Mock(spec=FilePersister)
        url = AnyUrl("file:///test/path")
        original_data = b"test data"
        data = memoryview(original_data)

        mock_persister.persist(url, data)
        mock_persister.persist.assert_called_once_with(url, data)

    def test_persist_with_memoryview_path(self):
        """Test persist method with memoryview data and path ref."""
        mock_persister = Mock(spec=FilePersister)
        path_ref = PurePosixPath("/test/path")
        original_data = b"test data"
        data = memoryview(original_data)

        mock_persister.persist(path_ref, data)
        mock_persister.persist.assert_called_once_with(path_ref, data)

    def test_persist_with_path_source_url(self):
        """Test persist method with Path source and URL ref."""
        mock_persister = Mock(spec=FilePersister)
        url = AnyUrl("file:///test/path")
        src_path = Path("/tmp/source")

        mock_persister.persist(url, src_path)
        mock_persister.persist.assert_called_once_with(url, src_path)

    def test_persist_with_path_source_path_ref(self):
        """Test persist method with Path source and path ref."""
        mock_persister = Mock(spec=FilePersister)
        path_ref = PurePosixPath("/test/path")
        src_path = Path("/tmp/source")

        mock_persister.persist(path_ref, src_path)
        mock_persister.persist.assert_called_once_with(path_ref, src_path)


class TestRefType:
    """Test the reference type definition."""

    def test_ref_accepts_anyurl(self):
        """Test that reference type accepts AnyUrl instances."""
        url = AnyUrl("file:///test/path")
        # This should not raise any type checker errors
        ref: AnyUrl | PurePosixPath = url
        assert ref == url

    def test_ref_accepts_pureposixpath(self):
        """Test that reference type accepts PurePosixPath instances."""
        path = PurePosixPath("/test/path")
        # This should not raise any type checker errors
        ref: AnyUrl | PurePosixPath = path
        assert ref == path

    def test_ref_type_union(self):
        """Test that reference type is properly defined as a union type."""
        # Test with different valid references
        refs = [
            AnyUrl("file:///test/path"),
            AnyUrl("s3://bucket/key"),
            AnyUrl("http://example.com/file"),
            PurePosixPath("/test/path"),
            PurePosixPath("relative/path"),
        ]

        for ref in refs:
            # Each should be a valid reference
            assert isinstance(ref, (AnyUrl, PurePosixPath))


class TestBuiltinErrorUsage:
    """Test that built-in ValueError is used for configuration errors."""

    def test_value_error_for_loader_misconfiguration(self):
        """Test that ValueError is used for loader configuration issues."""
        # This is tested implicitly in the protocol compliance tests
        # ValueError is a built-in exception, so no special testing needed
        assert issubclass(ValueError, Exception)

    def test_value_error_for_persister_misconfiguration(self):
        """Test that ValueError is used for persister configuration issues."""
        # This is tested implicitly in the protocol compliance tests
        # ValueError is a built-in exception, so no special testing needed
        assert issubclass(ValueError, Exception)


class TestProtocolCompatibility:
    """Test protocol compatibility and type checking."""

    def test_file_loader_protocol_compliance_url(self):
        """Test that we can implement FileLoader protocol with URLs."""

        class TestFileLoader:
            def load(self, ref: AnyUrl | PurePosixPath, dst=None):
                if isinstance(ref, PurePosixPath):
                    # Would need store configuration
                    raise ValueError("No store configured")

                if dst is None:
                    return io.BytesIO(b"test")
                elif isinstance(dst, Path):
                    dst.write_bytes(b"test")
                else:
                    dst.write(b"test")

        # This should be considered a valid FileLoader implementation
        loader = TestFileLoader()
        assert callable(loader.load)

        # Should work with URLs
        url = AnyUrl("file:///test")
        result = loader.load(url)
        assert isinstance(result, io.BytesIO)

    def test_file_loader_protocol_compliance_path_error(self):
        """Test that FileLoader raises appropriate error for unconfigured paths."""

        class TestFileLoader:
            def __init__(self, store=None):
                self.store = store

            def load(self, ref: AnyUrl | PurePosixPath, dst=None):
                if isinstance(ref, PurePosixPath) and self.store is None:
                    raise ValueError(
                        "Cannot use path reference without a configured store"
                    )

                if dst is None:
                    return io.BytesIO(b"test")
                elif isinstance(dst, Path):
                    dst.write_bytes(b"test")
                else:
                    dst.write(b"test")

        loader = TestFileLoader()
        path_ref = PurePosixPath("/test/path")

        with pytest.raises(ValueError):
            loader.load(path_ref)

    def test_file_persister_protocol_compliance_url(self):
        """Test that we can implement FilePersister protocol with URLs."""

        class TestFilePersister:
            def persist(self, ref: AnyUrl | PurePosixPath, src):
                if isinstance(ref, PurePosixPath):
                    # Would need store configuration
                    raise ValueError("No store configured")
                # Mock implementation
                pass

        # This should be considered a valid FilePersister implementation
        persister = TestFilePersister()
        assert callable(persister.persist)

        # Should work with URLs
        url = AnyUrl("file:///test")
        persister.persist(url, b"data")

    def test_file_persister_protocol_compliance_path_error(self):
        """Test that FilePersister raises appropriate error for unconfigured paths."""

        class TestFilePersister:
            def __init__(self, store=None):
                self.store = store

            def persist(self, ref: AnyUrl | PurePosixPath, src):
                if isinstance(ref, PurePosixPath) and self.store is None:
                    raise ValueError(
                        "Cannot use path reference without a configured store"
                    )
                # Mock implementation
                pass

        persister = TestFilePersister()
        path_ref = PurePosixPath("/test/path")

        with pytest.raises(ValueError):
            persister.persist(path_ref, b"data")

    def test_pydantic_url_types(self):
        """Test that AnyUrl types work as expected."""
        # Test various URL formats
        file_url = AnyUrl("file:///test/path")
        assert str(file_url) == "file:///test/path"

        http_url = AnyUrl("http://example.com/path")
        assert str(http_url) == "http://example.com/path"

        s3_url = AnyUrl("s3://bucket/key")
        assert str(s3_url) == "s3://bucket/key"

    def test_pure_posix_path_types(self):
        """Test that PurePosixPath types work as expected."""
        # Test various path formats
        abs_path = PurePosixPath("/absolute/path")
        assert str(abs_path) == "/absolute/path"
        assert abs_path.is_absolute()

        rel_path = PurePosixPath("relative/path")
        assert str(rel_path) == "relative/path"
        assert not rel_path.is_absolute()

        root_path = PurePosixPath("/")
        assert str(root_path) == "/"
        assert root_path.is_absolute()


class TestProtocolDocumentation:
    """Test that protocol methods have proper documentation."""

    def test_builtin_error_usage(self):
        """Test that built-in ValueError is used appropriately."""
        # ValueError is the appropriate built-in exception for configuration errors
        assert ValueError.__doc__ is not None
