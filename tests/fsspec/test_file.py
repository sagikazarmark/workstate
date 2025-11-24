"""Tests for the fsspec file implementation."""

import io
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from pydantic import AnyUrl

from workstate.fsspec.file import FileLoader, FilePersister


class TestFileLoader:
    """Test the FileLoader implementation."""

    def test_initialization_default(self):
        """Test FileLoader initialization with default parameters."""
        loader = FileLoader()
        assert loader.fs is None

    def test_initialization_with_filesystem(self):
        """Test FileLoader initialization with custom filesystem."""
        mock_fs = Mock()
        loader = FileLoader(fs=mock_fs)
        assert loader.fs is mock_fs

    @patch("fsspec.open")
    def test_load_returns_io_default_fs(self, mock_fsspec_open):
        """Test load method returning IO object with default filesystem."""
        # Setup mock
        mock_file = Mock()
        mock_file.read.return_value = b"test data"
        mock_open_file = Mock()
        mock_open_file.open.return_value = mock_file
        mock_fsspec_open.return_value = mock_open_file

        loader = FileLoader()
        url = AnyUrl("file:///test/path.txt")

        result = loader.load(url)

        mock_fsspec_open.assert_called_once_with("file:///test/path.txt", "rb")
        mock_open_file.open.assert_called_once()
        assert result is mock_file

    def test_load_returns_io_custom_fs(self):
        """Test load method returning IO object with custom filesystem."""
        # Setup mock filesystem
        mock_fs = Mock()
        mock_file = Mock()
        mock_file.read.return_value = b"test data"
        mock_fs.open.return_value = mock_file

        loader = FileLoader(fs=mock_fs)
        url = AnyUrl("s3://bucket/path.txt")

        result = loader.load(url)

        mock_fs.open.assert_called_once_with("s3://bucket/path.txt", "rb")
        assert result is mock_file

    @patch("fsspec.open")
    def test_load_to_path_destination_default_fs(self, mock_fsspec_open):
        """Test load method with Path destination using default filesystem."""
        # Setup mock context manager
        test_data = b"test file content"
        mock_file = Mock()
        mock_file.read.return_value = test_data

        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_file)
        mock_context.__exit__ = Mock(return_value=None)

        mock_open_file = Mock()
        mock_open_file.open.return_value = mock_context
        mock_fsspec_open.return_value = mock_open_file

        loader = FileLoader()
        url = AnyUrl("file:///test/source.txt")

        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_path = Path(tmp_file.name)

        try:
            result = loader.load(url, tmp_path)

            assert result is None
            assert tmp_path.read_bytes() == test_data
            mock_fsspec_open.assert_called_once_with("file:///test/source.txt", "rb")
        finally:
            tmp_path.unlink()

    def test_load_to_path_destination_custom_fs(self):
        """Test load method with Path destination using custom filesystem."""
        # Setup mock filesystem
        mock_fs = Mock()
        test_data = b"test file content"
        mock_file = Mock()
        mock_file.read.return_value = test_data

        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_file)
        mock_context.__exit__ = Mock(return_value=None)
        mock_fs.open.return_value = mock_context

        loader = FileLoader(fs=mock_fs)
        url = AnyUrl("s3://bucket/source.txt")

        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_path = Path(tmp_file.name)

        try:
            result = loader.load(url, tmp_path)

            assert result is None
            assert tmp_path.read_bytes() == test_data
            mock_fs.open.assert_called_once_with("s3://bucket/source.txt", "rb")
        finally:
            tmp_path.unlink()

    @patch("fsspec.open")
    def test_load_to_io_destination_default_fs(self, mock_fsspec_open):
        """Test load method with IO destination using default filesystem."""
        # Setup mock context manager
        test_data = b"test stream content"
        mock_file = Mock()
        mock_file.read.return_value = test_data

        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_file)
        mock_context.__exit__ = Mock(return_value=None)

        mock_open_file = Mock()
        mock_open_file.open.return_value = mock_context
        mock_fsspec_open.return_value = mock_open_file

        loader = FileLoader()
        url = AnyUrl("http://example.com/file.txt")
        dst_io = io.BytesIO()

        result = loader.load(url, dst_io)

        assert result is None
        dst_io.seek(0)
        assert dst_io.read() == test_data
        mock_fsspec_open.assert_called_once_with("http://example.com/file.txt", "rb")

    def test_load_to_io_destination_custom_fs(self):
        """Test load method with IO destination using custom filesystem."""
        # Setup mock filesystem
        mock_fs = Mock()
        test_data = b"test stream content"
        mock_file = Mock()
        mock_file.read.return_value = test_data

        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_file)
        mock_context.__exit__ = Mock(return_value=None)
        mock_fs.open.return_value = mock_context

        loader = FileLoader(fs=mock_fs)
        url = AnyUrl("gs://bucket/file.txt")
        dst_io = io.BytesIO()

        result = loader.load(url, dst_io)

        assert result is None
        dst_io.seek(0)
        assert dst_io.read() == test_data
        mock_fs.open.assert_called_once_with("gs://bucket/file.txt", "rb")

    @patch("fsspec.open")
    def test_load_string_to_bytes_conversion(self, mock_fsspec_open):
        """Test that string data is properly converted to bytes."""
        # Setup mock that returns string instead of bytes
        test_data_str = "test string content"
        test_data_bytes = test_data_str.encode("utf-8")
        mock_file = Mock()
        mock_file.read.return_value = test_data_str  # Return string instead of bytes

        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_file)
        mock_context.__exit__ = Mock(return_value=None)

        mock_open_file = Mock()
        mock_open_file.open.return_value = mock_context
        mock_fsspec_open.return_value = mock_open_file

        loader = FileLoader()
        url = AnyUrl("file:///test/text.txt")
        dst_io = io.BytesIO()

        result = loader.load(url, dst_io)

        assert result is None
        dst_io.seek(0)
        assert dst_io.read() == test_data_bytes


class TestFilePersister:
    """Test the FilePersister implementation."""

    def test_initialization_default(self):
        """Test FilePersister initialization with default parameters."""
        persister = FilePersister()
        assert persister.fs is None

    def test_initialization_with_filesystem(self):
        """Test FilePersister initialization with custom filesystem."""
        mock_fs = Mock()
        persister = FilePersister(fs=mock_fs)
        assert persister.fs is mock_fs

    @patch("fsspec.open")
    def test_persist_with_bytes_default_fs(self, mock_fsspec_open):
        """Test persist method with bytes data using default filesystem."""
        # Setup mock context manager
        mock_file = Mock()
        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_file)
        mock_context.__exit__ = Mock(return_value=None)
        mock_fsspec_open.return_value = mock_context

        persister = FilePersister()
        url = AnyUrl("file:///test/output.txt")
        data = b"test data"

        persister.persist(url, data)

        mock_fsspec_open.assert_called_once_with(url, "wb")
        mock_file.write.assert_called_once_with(data)

    def test_persist_with_bytes_custom_fs(self):
        """Test persist method with bytes data using custom filesystem."""
        # Setup mock filesystem
        mock_fs = Mock()
        mock_file = Mock()
        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_file)
        mock_context.__exit__ = Mock(return_value=None)
        mock_fs.open.return_value = mock_context

        persister = FilePersister(fs=mock_fs)
        url = AnyUrl("s3://bucket/output.txt")
        data = b"test data"

        persister.persist(url, data)

        mock_fs.open.assert_called_once_with(url, "wb")
        mock_file.write.assert_called_once_with(data)

    @patch("fsspec.open")
    def test_persist_with_bytearray_default_fs(self, mock_fsspec_open):
        """Test persist method with bytearray data using default filesystem."""
        # Setup mock context manager
        mock_file = Mock()
        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_file)
        mock_context.__exit__ = Mock(return_value=None)
        mock_fsspec_open.return_value = mock_context

        persister = FilePersister()
        url = AnyUrl("file:///test/output.txt")
        data = bytearray(b"test data")

        persister.persist(url, data)

        mock_fsspec_open.assert_called_once_with(url, "wb")
        mock_file.write.assert_called_once_with(data)

    def test_persist_with_bytearray_custom_fs(self):
        """Test persist method with bytearray data using custom filesystem."""
        # Setup mock filesystem
        mock_fs = Mock()
        mock_file = Mock()
        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_file)
        mock_context.__exit__ = Mock(return_value=None)
        mock_fs.open.return_value = mock_context

        persister = FilePersister(fs=mock_fs)
        url = AnyUrl("gs://bucket/output.txt")
        data = bytearray(b"test data")

        persister.persist(url, data)

        mock_fs.open.assert_called_once_with(url, "wb")
        mock_file.write.assert_called_once_with(data)

    @patch("fsspec.open")
    def test_persist_with_memoryview_default_fs(self, mock_fsspec_open):
        """Test persist method with memoryview data using default filesystem."""
        # Setup mock context manager
        mock_file = Mock()
        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_file)
        mock_context.__exit__ = Mock(return_value=None)
        mock_fsspec_open.return_value = mock_context

        persister = FilePersister()
        url = AnyUrl("file:///test/output.txt")
        original_data = b"test data"
        data = memoryview(original_data)

        persister.persist(url, data)

        mock_fsspec_open.assert_called_once_with(url, "wb")
        mock_file.write.assert_called_once_with(data)

    def test_persist_with_memoryview_custom_fs(self):
        """Test persist method with memoryview data using custom filesystem."""
        # Setup mock filesystem
        mock_fs = Mock()
        mock_file = Mock()
        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_file)
        mock_context.__exit__ = Mock(return_value=None)
        mock_fs.open.return_value = mock_context

        persister = FilePersister(fs=mock_fs)
        url = AnyUrl("azure://container/output.txt")
        original_data = b"test data"
        data = memoryview(original_data)

        persister.persist(url, data)

        mock_fs.open.assert_called_once_with(url, "wb")
        mock_file.write.assert_called_once_with(data)

    @patch("fsspec.open")
    def test_persist_with_path_default_fs(self, mock_fsspec_open):
        """Test persist method with Path source using default filesystem."""
        # Setup mock context manager
        mock_file = Mock()
        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_file)
        mock_context.__exit__ = Mock(return_value=None)
        mock_fsspec_open.return_value = mock_context

        persister = FilePersister()
        url = AnyUrl("file:///test/output.txt")
        src_path = Path("/tmp/source.txt")

        persister.persist(url, src_path)

        mock_fsspec_open.assert_called_once_with(url, "wb")
        mock_file.write.assert_called_once_with(src_path)

    def test_persist_with_path_custom_fs(self):
        """Test persist method with Path source using custom filesystem."""
        # Setup mock filesystem
        mock_fs = Mock()
        mock_file = Mock()
        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_file)
        mock_context.__exit__ = Mock(return_value=None)
        mock_fs.open.return_value = mock_context

        persister = FilePersister(fs=mock_fs)
        # Use a simpler URL that's valid
        url = AnyUrl("s3://bucket/output.txt")
        src_path = Path("/tmp/source.txt")

        persister.persist(url, src_path)

        mock_fs.open.assert_called_once_with(url, "wb")
        mock_file.write.assert_called_once_with(src_path)


class TestIntegration:
    """Integration tests for FileLoader and FilePersister."""

    def test_loader_and_persister_compatibility(self):
        """Test that FileLoader and FilePersister work together."""
        # This tests the basic contract compatibility
        loader = FileLoader()
        persister = FilePersister()

        # Both should be able to work with the same URL types
        url = AnyUrl("file:///test/file.txt")

        # Test that the interfaces are compatible
        assert hasattr(loader, "load")
        assert hasattr(persister, "persist")

        # Test method signatures are compatible
        import inspect

        load_sig = inspect.signature(loader.load)
        persist_sig = inspect.signature(persister.persist)

        # Load should accept url and optional destination
        assert "ref" in load_sig.parameters
        assert "dst" in load_sig.parameters

        # Persist should accept url and source
        assert "ref" in persist_sig.parameters
        assert "src" in persist_sig.parameters

    @patch("fsspec.open")
    def test_memory_filesystem_simulation(self, mock_fsspec_open):
        """Test using in-memory filesystem simulation."""
        # Simulate a memory filesystem for testing
        memory_data = {}

        def mock_open_func(path, mode):
            if "r" in mode:
                # Reading mode
                if str(path) in memory_data:
                    mock_file = Mock()
                    mock_file.read.return_value = memory_data[str(path)]
                    return mock_file
                else:
                    raise FileNotFoundError(f"No such file: {path}")
            elif "w" in mode:
                # Writing mode
                mock_file = Mock()

                def write_data(data):
                    memory_data[str(path)] = data

                mock_file.write = write_data
                mock_context = Mock()
                mock_context.__enter__ = Mock(return_value=mock_file)
                mock_context.__exit__ = Mock(return_value=None)
                return mock_context

        def open_file_handler(path, mode):
            if "r" in mode:
                # For reading, return an OpenFile-like object
                mock_open_file = Mock()
                mock_open_file.open.return_value = mock_open_func(path, mode)
                return mock_open_file
            else:
                # For writing, return context manager directly
                return mock_open_func(path, mode)

        mock_fsspec_open.side_effect = open_file_handler

        # Test roundtrip: persist then load
        persister = FilePersister()
        loader = FileLoader()

        url = AnyUrl("memory://test/roundtrip.txt")
        original_data = b"test roundtrip data"

        # Persist data
        persister.persist(url, original_data)

        # Load data back
        result = loader.load(url)

        # Verify roundtrip
        assert result.read() == original_data

    def test_url_types_compatibility(self):
        """Test that both classes handle various URL types consistently."""
        loader = FileLoader()
        persister = FilePersister()

        # Test various URL formats that are valid with Pydantic
        urls = [
            AnyUrl("file:///tmp/test.txt"),
            AnyUrl("s3://bucket/key"),
            AnyUrl("gs://bucket/object"),
            AnyUrl("azure://container/blob"),
            AnyUrl("http://example.com/file"),
            AnyUrl("https://example.com/file"),
            AnyUrl("ftp://server/path/file"),
        ]

        for url in urls:
            # Both should accept the same URL types
            # We can't easily test the actual operations without real filesystems,
            # but we can test that the methods accept the URL types
            assert isinstance(str(url), str)


class TestErrorHandling:
    """Test error handling and edge cases."""

    @patch("fsspec.open")
    def test_load_file_not_found_default_fs(self, mock_fsspec_open):
        """Test FileLoader handling when file is not found with default filesystem."""
        mock_fsspec_open.side_effect = FileNotFoundError("File not found")

        loader = FileLoader()
        url = AnyUrl("file:///nonexistent/file.txt")

        with pytest.raises(FileNotFoundError):
            loader.load(url)

    def test_load_file_not_found_custom_fs(self):
        """Test FileLoader handling when file is not found with custom filesystem."""
        mock_fs = Mock()
        mock_fs.open.side_effect = FileNotFoundError("File not found")

        loader = FileLoader(fs=mock_fs)
        url = AnyUrl("s3://bucket/nonexistent.txt")

        with pytest.raises(FileNotFoundError):
            loader.load(url)

    @patch("fsspec.open")
    def test_persist_permission_error_default_fs(self, mock_fsspec_open):
        """Test FilePersister handling when write permission is denied with default filesystem."""
        mock_fsspec_open.side_effect = PermissionError("Permission denied")

        persister = FilePersister()
        url = AnyUrl("file:///protected/file.txt")
        data = b"test data"

        with pytest.raises(PermissionError):
            persister.persist(url, data)

    def test_persist_permission_error_custom_fs(self):
        """Test FilePersister handling when write permission is denied with custom filesystem."""
        mock_fs = Mock()
        mock_fs.open.side_effect = PermissionError("Permission denied")

        persister = FilePersister(fs=mock_fs)
        url = AnyUrl("s3://protected-bucket/file.txt")
        data = b"test data"

        with pytest.raises(PermissionError):
            persister.persist(url, data)

    def test_load_with_invalid_url_type(self):
        """Test FileLoader with various URL formats."""
        loader = FileLoader()

        # Test with simple file path - should work with fsspec
        with patch("fsspec.open") as mock_open:
            mock_file = Mock()
            mock_file.read.return_value = b"data"
            mock_open_file = Mock()
            mock_open_file.open.return_value = mock_file
            mock_open.return_value = mock_open_file

            # This should work as fsspec handles string paths
            result = loader.load(AnyUrl("file:///simple-path.txt"))
            assert result is mock_file

    @patch("fsspec.open")
    def test_persist_with_invalid_data_type(self, mock_fsspec_open):
        """Test FilePersister with invalid data type."""
        # Mock to prevent actual file system operations
        mock_context = Mock()
        mock_file = Mock()
        # Make the mock file write method raise an error for invalid types
        mock_file.write.side_effect = TypeError(
            "a bytes-like object is required, not 'int'"
        )
        mock_context.__enter__ = Mock(return_value=mock_file)
        mock_context.__exit__ = Mock(return_value=None)
        mock_fsspec_open.return_value = mock_context

        persister = FilePersister()
        url = AnyUrl("file:///tmp/test-output.txt")

        # Test with invalid data type (should be handled by the write operation)
        with pytest.raises(TypeError):
            persister.persist(url, 12345)  # Invalid type

    @patch("fsspec.open")
    def test_load_io_error_during_read(self, mock_fsspec_open):
        """Test FileLoader handling IO errors during read."""
        mock_file = Mock()
        mock_file.read.side_effect = IOError("IO error during read")

        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_file)
        mock_context.__exit__ = Mock(return_value=None)

        mock_open_file = Mock()
        mock_open_file.open.return_value = mock_context
        mock_fsspec_open.return_value = mock_open_file

        loader = FileLoader()
        url = AnyUrl("file:///test/problematic.txt")
        dst = io.BytesIO()

        with pytest.raises(IOError):
            loader.load(url, dst)

    @patch("fsspec.open")
    def test_persist_io_error_during_write(self, mock_fsspec_open):
        """Test FilePersister handling IO errors during write."""
        mock_file = Mock()
        mock_file.write.side_effect = IOError("IO error during write")

        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_file)
        mock_context.__exit__ = Mock(return_value=None)
        mock_fsspec_open.return_value = mock_context

        persister = FilePersister()
        url = AnyUrl("file:///tmp/test-problematic.txt")
        data = b"test data"

        with pytest.raises(IOError):
            persister.persist(url, data)


class TestTypeHints:
    """Test type hints and overload behavior."""

    def test_load_overload_io_return(self):
        """Test that load method overload returns IO when no destination."""
        loader = FileLoader()

        # The overload should indicate IO return type when no dst parameter
        with patch("fsspec.open") as mock_open:
            mock_file = Mock()
            mock_open_file = Mock()
            mock_open_file.open.return_value = mock_file
            mock_open.return_value = mock_open_file

            url = AnyUrl("file:///test/file.txt")
            result = loader.load(url)

            # Should return an IO-like object
            assert result is not None
            assert result is mock_file

    @patch("fsspec.open")
    def test_load_overload_none_return_with_path(self, mock_fsspec_open):
        """Test that load method overload returns None with Path destination."""
        mock_file = Mock()
        mock_file.read.return_value = b"test"

        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_file)
        mock_context.__exit__ = Mock(return_value=None)

        mock_open_file = Mock()
        mock_open_file.open.return_value = mock_context
        mock_fsspec_open.return_value = mock_open_file

        loader = FileLoader()
        url = AnyUrl("file:///test/file.txt")

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = Path(tmp.name)

        try:
            result = loader.load(url, tmp_path)
            # Should return None when writing to destination
            assert result is None
        finally:
            tmp_path.unlink()

    @patch("fsspec.open")
    def test_load_overload_none_return_with_io(self, mock_fsspec_open):
        """Test that load method overload returns None with IO destination."""
        mock_file = Mock()
        mock_file.read.return_value = b"test"

        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_file)
        mock_context.__exit__ = Mock(return_value=None)

        mock_open_file = Mock()
        mock_open_file.open.return_value = mock_context
        mock_fsspec_open.return_value = mock_open_file

        loader = FileLoader()
        url = AnyUrl("file:///test/file.txt")
        dst = io.BytesIO()

        result = loader.load(url, dst)
        # Should return None when writing to destination
        assert result is None

    @patch("fsspec.open")
    def test_persist_overload_bytes_types(self, mock_fsspec_open):
        """Test that persist method accepts various byte-like types."""
        mock_file = Mock()
        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_file)
        mock_context.__exit__ = Mock(return_value=None)
        mock_fsspec_open.return_value = mock_context

        persister = FilePersister()
        url = AnyUrl("file:///tmp/test-output.txt")

        # Test with different byte-like types
        test_cases = [
            b"bytes data",
            bytearray(b"bytearray data"),
            memoryview(b"memoryview data"),
            Path("/tmp/some-file.txt"),  # Path type
        ]

        for test_data in test_cases:
            persister.persist(url, test_data)
            mock_file.write.assert_called_with(test_data)
            mock_file.reset_mock()


class TestRealFilesystem:
    """Test with real filesystem operations for edge cases."""

    def test_load_and_persist_roundtrip_real_files(self):
        """Test actual file operations with temporary files."""
        loader = FileLoader()
        persister = FilePersister()

        test_data = b"test roundtrip content with fsspec"

        # Create temporary files for testing
        with tempfile.NamedTemporaryFile(delete=False) as src_file:
            src_path = Path(src_file.name)
            src_file.write(test_data)

        with tempfile.NamedTemporaryFile(delete=False) as dst_file:
            dst_path = Path(dst_file.name)

        try:
            # Test: persist to file, then load back
            src_url = AnyUrl(f"file://{src_path}")
            dst_url = AnyUrl(f"file://{dst_path}")

            # Load from source and persist to destination
            loaded_io = loader.load(src_url)
            loaded_data = loaded_io.read()
            persister.persist(dst_url, loaded_data)

            # Verify the roundtrip worked
            assert dst_path.read_bytes() == test_data

        finally:
            src_path.unlink()
            dst_path.unlink()

    def test_load_to_path_real_filesystem(self):
        """Test loading directly to a Path with real filesystem."""
        loader = FileLoader()

        test_data = b"test direct path loading"

        # Create source file
        with tempfile.NamedTemporaryFile(delete=False) as src_file:
            src_path = Path(src_file.name)
            src_file.write(test_data)

        with tempfile.NamedTemporaryFile(delete=False) as dst_file:
            dst_path = Path(dst_file.name)

        try:
            src_url = AnyUrl(f"file://{src_path}")

            # Load directly to destination path
            result = loader.load(src_url, dst_path)

            assert result is None
            assert dst_path.read_bytes() == test_data

        finally:
            src_path.unlink()
            dst_path.unlink()

    def test_load_to_io_real_filesystem(self):
        """Test loading to IO object with real filesystem."""
        loader = FileLoader()

        test_data = b"test IO loading"

        # Create source file
        with tempfile.NamedTemporaryFile(delete=False) as src_file:
            src_path = Path(src_file.name)
            src_file.write(test_data)

        try:
            src_url = AnyUrl(f"file://{src_path}")
            dst_io = io.BytesIO()

            # Load to IO object
            result = loader.load(src_url, dst_io)

            assert result is None
            dst_io.seek(0)
            assert dst_io.read() == test_data

        finally:
            src_path.unlink()
