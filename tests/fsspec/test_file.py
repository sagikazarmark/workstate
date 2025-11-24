"""Tests for the fsspec file implementation."""

import io
import tempfile
from pathlib import Path, PurePosixPath
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
        url = AnyUrl("s3://bucket/key.txt")

        result = loader.load(url)

        mock_fs.open.assert_called_once_with("s3://bucket/key.txt", "rb")
        assert result is mock_file

    def test_load_returns_io_path_with_fs(self):
        """Test load method returning IO object with path reference and configured filesystem."""
        # Setup mock filesystem
        mock_fs = Mock()
        mock_file = Mock()
        mock_file.read.return_value = b"test data"
        mock_fs.open.return_value = mock_file

        loader = FileLoader(fs=mock_fs)
        path_ref = PurePosixPath("/test/path.txt")

        result = loader.load(path_ref)

        mock_fs.open.assert_called_once_with("/test/path.txt", "rb")
        assert result is mock_file

    def test_load_returns_io_path_without_fs_raises_error(self):
        """Test load method with path reference but no filesystem raises error."""
        loader = FileLoader()
        path_ref = PurePosixPath("/test/path.txt")

        with pytest.raises(ValueError) as exc_info:
            loader.load(path_ref)

        assert "Cannot use path reference without a configured filesystem" in str(
            exc_info.value
        )

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

    def test_load_to_path_destination_path_ref_with_fs(self):
        """Test load method with Path destination and path reference using configured filesystem."""
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
        path_ref = PurePosixPath("/source/path.txt")

        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_path = Path(tmp_file.name)

        try:
            result = loader.load(path_ref, tmp_path)

            assert result is None
            assert tmp_path.read_bytes() == test_data
            mock_fs.open.assert_called_once_with("/source/path.txt", "rb")
        finally:
            tmp_path.unlink()

    def test_load_to_path_destination_path_ref_without_fs_raises_error(self):
        """Test load method with Path destination and path reference but no filesystem raises error."""
        loader = FileLoader()
        path_ref = PurePosixPath("/source/path.txt")
        tmp_path = Path("/tmp/test.txt")

        with pytest.raises(ValueError) as exc_info:
            loader.load(path_ref, tmp_path)

        assert "Cannot use path reference without a configured filesystem" in str(
            exc_info.value
        )

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
        url = AnyUrl("gcs://bucket/file.txt")
        dst_io = io.BytesIO()

        result = loader.load(url, dst_io)

        assert result is None
        dst_io.seek(0)
        assert dst_io.read() == test_data
        mock_fs.open.assert_called_once_with("gcs://bucket/file.txt", "rb")

    def test_load_to_io_destination_path_ref_with_fs(self):
        """Test load method with IO destination and path reference using configured filesystem."""
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
        path_ref = PurePosixPath("/source/file.txt")
        dst_io = io.BytesIO()

        result = loader.load(path_ref, dst_io)

        assert result is None
        dst_io.seek(0)
        assert dst_io.read() == test_data
        mock_fs.open.assert_called_once_with("/source/file.txt", "rb")

    def test_load_to_io_destination_path_ref_without_fs_raises_error(self):
        """Test load method with IO destination and path reference but no filesystem raises error."""
        loader = FileLoader()
        path_ref = PurePosixPath("/source/file.txt")
        dst_io = io.BytesIO()

        with pytest.raises(ValueError) as exc_info:
            loader.load(path_ref, dst_io)

        assert "Cannot use path reference without a configured filesystem" in str(
            exc_info.value
        )

    def test_load_string_to_bytes_conversion(self):
        """Test that string data is converted to bytes when loading."""
        # Setup mock filesystem that returns string data
        mock_fs = Mock()
        string_data = "test string content"
        mock_file = Mock()
        mock_file.read.return_value = string_data

        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_file)
        mock_context.__exit__ = Mock(return_value=None)
        mock_fs.open.return_value = mock_context

        loader = FileLoader(fs=mock_fs)
        url = AnyUrl("custom://path/file.txt")
        dst_io = io.BytesIO()

        result = loader.load(url, dst_io)

        assert result is None
        dst_io.seek(0)
        # String should be converted to bytes
        assert dst_io.read() == string_data.encode("utf-8")


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
        mock_file = Mock()
        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_file)
        mock_context.__exit__ = Mock(return_value=None)
        mock_fsspec_open.return_value = mock_context

        persister = FilePersister()
        url = AnyUrl("file:///test/path.txt")
        data = b"test data"

        persister.persist(url, data)

        mock_fsspec_open.assert_called_once_with("file:///test/path.txt", "wb")
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
        url = AnyUrl("s3://bucket/key.txt")
        data = b"test data"

        persister.persist(url, data)

        mock_fs.open.assert_called_once_with("s3://bucket/key.txt", "wb")
        mock_file.write.assert_called_once_with(data)

    def test_persist_with_bytes_path_ref_and_fs(self):
        """Test persist method with bytes data, path reference, and configured filesystem."""
        # Setup mock filesystem
        mock_fs = Mock()
        mock_file = Mock()
        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_file)
        mock_context.__exit__ = Mock(return_value=None)
        mock_fs.open.return_value = mock_context

        persister = FilePersister(fs=mock_fs)
        path_ref = PurePosixPath("/test/path.txt")
        data = b"test data"

        persister.persist(path_ref, data)

        mock_fs.open.assert_called_once_with("/test/path.txt", "wb")
        mock_file.write.assert_called_once_with(data)

    def test_persist_with_bytes_path_ref_without_fs_raises_error(self):
        """Test persist method with bytes data and path reference but no filesystem raises error."""
        persister = FilePersister()
        path_ref = PurePosixPath("/test/path.txt")
        data = b"test data"

        with pytest.raises(ValueError) as exc_info:
            persister.persist(path_ref, data)

        assert "Cannot use path reference without a configured filesystem" in str(
            exc_info.value
        )

    @patch("fsspec.open")
    def test_persist_with_bytearray_default_fs(self, mock_fsspec_open):
        """Test persist method with bytearray data using default filesystem."""
        mock_file = Mock()
        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_file)
        mock_context.__exit__ = Mock(return_value=None)
        mock_fsspec_open.return_value = mock_context

        persister = FilePersister()
        url = AnyUrl("file:///test/path.txt")
        data = bytearray(b"test data")

        persister.persist(url, data)

        mock_fsspec_open.assert_called_once_with("file:///test/path.txt", "wb")
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
        url = AnyUrl("s3://bucket/key.txt")
        data = bytearray(b"test data")

        persister.persist(url, data)

        mock_fs.open.assert_called_once_with("s3://bucket/key.txt", "wb")
        mock_file.write.assert_called_once_with(data)

    def test_persist_with_bytearray_path_ref_and_fs(self):
        """Test persist method with bytearray data, path reference, and configured filesystem."""
        # Setup mock filesystem
        mock_fs = Mock()
        mock_file = Mock()
        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_file)
        mock_context.__exit__ = Mock(return_value=None)
        mock_fs.open.return_value = mock_context

        persister = FilePersister(fs=mock_fs)
        path_ref = PurePosixPath("/test/path.txt")
        data = bytearray(b"test data")

        persister.persist(path_ref, data)

        mock_fs.open.assert_called_once_with("/test/path.txt", "wb")
        mock_file.write.assert_called_once_with(data)

    @patch("fsspec.open")
    def test_persist_with_memoryview_default_fs(self, mock_fsspec_open):
        """Test persist method with memoryview data using default filesystem."""
        mock_file = Mock()
        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_file)
        mock_context.__exit__ = Mock(return_value=None)
        mock_fsspec_open.return_value = mock_context

        persister = FilePersister()
        url = AnyUrl("file:///test/path.txt")
        original_data = b"test data"
        data = memoryview(original_data)

        persister.persist(url, data)

        mock_fsspec_open.assert_called_once_with("file:///test/path.txt", "wb")
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
        url = AnyUrl("s3://bucket/key.txt")
        original_data = b"test data"
        data = memoryview(original_data)

        persister.persist(url, data)

        mock_fs.open.assert_called_once_with("s3://bucket/key.txt", "wb")
        mock_file.write.assert_called_once_with(data)

    def test_persist_with_memoryview_path_ref_and_fs(self):
        """Test persist method with memoryview data, path reference, and configured filesystem."""
        # Setup mock filesystem
        mock_fs = Mock()
        mock_file = Mock()
        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_file)
        mock_context.__exit__ = Mock(return_value=None)
        mock_fs.open.return_value = mock_context

        persister = FilePersister(fs=mock_fs)
        path_ref = PurePosixPath("/test/path.txt")
        original_data = b"test data"
        data = memoryview(original_data)

        persister.persist(path_ref, data)

        mock_fs.open.assert_called_once_with("/test/path.txt", "wb")
        mock_file.write.assert_called_once_with(data)

    @patch("fsspec.open")
    def test_persist_with_path_default_fs(self, mock_fsspec_open):
        """Test persist method with Path source using default filesystem."""
        # Create a temporary file to use as source
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            src_path = Path(tmp_file.name)
            test_data = b"test file content"
            tmp_file.write(test_data)

        try:
            mock_file = Mock()
            mock_context = Mock()
            mock_context.__enter__ = Mock(return_value=mock_file)
            mock_context.__exit__ = Mock(return_value=None)
            mock_fsspec_open.return_value = mock_context

            persister = FilePersister()
            url = AnyUrl("file:///test/dest.txt")

            persister.persist(url, src_path)

            mock_fsspec_open.assert_called_once_with("file:///test/dest.txt", "wb")
            mock_file.write.assert_called_once_with(test_data)
        finally:
            src_path.unlink()

    def test_persist_with_path_custom_fs(self):
        """Test persist method with Path source using custom filesystem."""
        # Create a temporary file to use as source
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            src_path = Path(tmp_file.name)
            test_data = b"test file content"
            tmp_file.write(test_data)

        try:
            # Setup mock filesystem
            mock_fs = Mock()
            mock_file = Mock()
            mock_context = Mock()
            mock_context.__enter__ = Mock(return_value=mock_file)
            mock_context.__exit__ = Mock(return_value=None)
            mock_fs.open.return_value = mock_context

            persister = FilePersister(fs=mock_fs)
            url = AnyUrl("s3://bucket/dest.txt")

            persister.persist(url, src_path)

            mock_fs.open.assert_called_once_with("s3://bucket/dest.txt", "wb")
            mock_file.write.assert_called_once_with(test_data)
        finally:
            src_path.unlink()

    def test_persist_with_path_source_path_ref_and_fs(self):
        """Test persist method with Path source, path reference, and configured filesystem."""
        # Create a temporary file to use as source
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            src_path = Path(tmp_file.name)
            test_data = b"test file content"
            tmp_file.write(test_data)

        try:
            # Setup mock filesystem
            mock_fs = Mock()
            mock_file = Mock()
            mock_context = Mock()
            mock_context.__enter__ = Mock(return_value=mock_file)
            mock_context.__exit__ = Mock(return_value=None)
            mock_fs.open.return_value = mock_context

            persister = FilePersister(fs=mock_fs)
            path_ref = PurePosixPath("/dest/path.txt")

            persister.persist(path_ref, src_path)

            mock_fs.open.assert_called_once_with("/dest/path.txt", "wb")
            mock_file.write.assert_called_once_with(test_data)
        finally:
            src_path.unlink()

    def test_persist_with_path_source_path_ref_without_fs_raises_error(self):
        """Test persist method with Path source and path reference but no filesystem raises error."""
        # Create a temporary file to use as source
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            src_path = Path(tmp_file.name)
            tmp_file.write(b"test data")

        try:
            persister = FilePersister()
            path_ref = PurePosixPath("/dest/path.txt")

            with pytest.raises(ValueError) as exc_info:
                persister.persist(path_ref, src_path)

            assert "Cannot use path reference without a configured filesystem" in str(
                exc_info.value
            )
        finally:
            src_path.unlink()


class TestIntegration:
    """Test integration scenarios."""

    def test_loader_and_persister_compatibility(self):
        """Test that FileLoader and FilePersister work together."""
        # Setup mock filesystem
        mock_fs = Mock()
        test_data = b"integration test data"

        # Mock for loading
        mock_read_file = Mock()
        mock_read_file.read.return_value = test_data
        mock_read_context = Mock()
        mock_read_context.__enter__ = Mock(return_value=mock_read_file)
        mock_read_context.__exit__ = Mock(return_value=None)

        # Mock for persisting
        mock_write_file = Mock()
        mock_write_context = Mock()
        mock_write_context.__enter__ = Mock(return_value=mock_write_file)
        mock_write_context.__exit__ = Mock(return_value=None)

        # Configure filesystem to return appropriate context managers
        mock_fs.open.side_effect = [mock_read_context, mock_write_context]

        loader = FileLoader(fs=mock_fs)
        persister = FilePersister(fs=mock_fs)

        source_path = PurePosixPath("/source/file.txt")
        dest_path = PurePosixPath("/dest/file.txt")

        # Load data
        data_io = loader.load(source_path)
        assert isinstance(data_io, Mock)

        # Persist data
        persister.persist(dest_path, test_data)

        # Verify calls
        assert mock_fs.open.call_count == 2
        mock_fs.open.assert_any_call("/source/file.txt", "rb")
        mock_fs.open.assert_any_call("/dest/file.txt", "wb")
        mock_write_file.write.assert_called_once_with(test_data)

    def test_mixed_url_and_path_references(self):
        """Test using both URL and path references in the same workflow."""
        # Setup mock filesystem for path operations
        mock_fs = Mock()
        test_data = b"mixed reference test"

        # Mock for path-based loading
        mock_path_file = Mock()
        mock_path_file.read.return_value = test_data
        mock_path_context = Mock()
        mock_path_context.__enter__ = Mock(return_value=mock_path_file)
        mock_path_context.__exit__ = Mock(return_value=None)
        mock_fs.open.return_value = mock_path_context

        loader = FileLoader(fs=mock_fs)
        persister = FilePersister(fs=mock_fs)

        # Load from path reference
        path_ref = PurePosixPath("/internal/data.txt")
        loaded_data = loader.load(path_ref)
        assert isinstance(loaded_data, Mock)

        # Persist to path reference
        dest_path = PurePosixPath("/internal/backup.txt")
        persister.persist(dest_path, test_data)

        # Verify filesystem operations
        mock_fs.open.assert_any_call("/internal/data.txt", "rb")
        mock_fs.open.assert_any_call("/internal/backup.txt", "wb")


class TestErrorHandling:
    """Test error handling scenarios."""

    def test_load_file_not_found_default_fs(self):
        """Test load method with file not found using default filesystem."""
        loader = FileLoader()
        url = AnyUrl("file:///nonexistent/path.txt")

        with patch("fsspec.open") as mock_fsspec_open:
            mock_fsspec_open.side_effect = FileNotFoundError("File not found")

            with pytest.raises(FileNotFoundError):
                loader.load(url)

    def test_load_file_not_found_custom_fs(self):
        """Test load method with file not found using custom filesystem."""
        mock_fs = Mock()
        mock_fs.open.side_effect = FileNotFoundError("File not found")

        loader = FileLoader(fs=mock_fs)
        path_ref = PurePosixPath("/nonexistent/path.txt")

        with pytest.raises(FileNotFoundError):
            loader.load(path_ref)

    def test_persist_permission_error_default_fs(self):
        """Test persist method with permission error using default filesystem."""
        persister = FilePersister()
        url = AnyUrl("file:///restricted/path.txt")
        data = b"test data"

        with patch("fsspec.open") as mock_fsspec_open:
            mock_fsspec_open.side_effect = PermissionError("Permission denied")

            with pytest.raises(PermissionError):
                persister.persist(url, data)

    def test_persist_permission_error_custom_fs(self):
        """Test persist method with permission error using custom filesystem."""
        mock_fs = Mock()
        mock_fs.open.side_effect = PermissionError("Permission denied")

        persister = FilePersister(fs=mock_fs)
        path_ref = PurePosixPath("/restricted/path.txt")
        data = b"test data"

        with pytest.raises(PermissionError):
            persister.persist(path_ref, data)

    def test_load_with_invalid_reference_type(self):
        """Test load method with invalid reference type."""
        loader = FileLoader()

        # This should be caught by type checker, but test runtime behavior
        with pytest.raises((AttributeError, FileNotFoundError)):
            # Pass an integer instead of Ref type
            loader.load(123)  # type: ignore

    def test_persist_with_invalid_data_type(self):
        """Test persist method with invalid data type."""
        persister = FilePersister()
        url = AnyUrl("file:///test/path.txt")

        with patch("fsspec.open") as mock_fsspec_open:
            mock_file = Mock()
            mock_context = Mock()
            mock_context.__enter__ = Mock(return_value=mock_file)
            mock_context.__exit__ = Mock(return_value=None)
            mock_open_file = Mock()
            mock_open_file.return_value = mock_context
            mock_fsspec_open.return_value = mock_open_file

            # This should be caught by type checker, but test runtime behavior
            mock_file.write.side_effect = TypeError("Invalid data type")

            with pytest.raises(TypeError):
                persister.persist(url, "string data")  # type: ignore


class TestRealFilesystem:
    """Test with real filesystem operations."""

    def test_load_and_persist_roundtrip_real_files(self):
        """Test loading and persisting with real files."""
        test_data = b"roundtrip test content"

        # Create source file
        with tempfile.NamedTemporaryFile(delete=False) as src_file:
            src_path = Path(src_file.name)
            src_file.write(test_data)

        # Create destination file path
        with tempfile.NamedTemporaryFile(delete=False) as dst_file:
            dst_path = Path(dst_file.name)

        try:
            loader = FileLoader()
            persister = FilePersister()

            # Create URLs from file paths
            src_url = AnyUrl(f"file://{src_path}")
            dst_url = AnyUrl(f"file://{dst_path}")

            # Load and persist
            loaded_io = loader.load(src_url)
            loaded_data = loaded_io.read()
            persister.persist(dst_url, loaded_data)

            # Verify roundtrip
            assert dst_path.read_bytes() == test_data

        finally:
            src_path.unlink()
            dst_path.unlink()

    def test_load_to_path_real_filesystem(self):
        """Test loading directly to a Path with real filesystem."""
        test_data = b"test direct path loading"

        # Create source file
        with tempfile.NamedTemporaryFile(delete=False) as src_file:
            src_path = Path(src_file.name)
            src_file.write(test_data)

        with tempfile.NamedTemporaryFile(delete=False) as dst_file:
            dst_path = Path(dst_file.name)

        try:
            loader = FileLoader()
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
        test_data = b"test IO loading"

        # Create source file
        with tempfile.NamedTemporaryFile(delete=False) as src_file:
            src_path = Path(src_file.name)
            src_file.write(test_data)

        try:
            loader = FileLoader()
            src_url = AnyUrl(f"file://{src_path}")
            dst_io = io.BytesIO()

            # Load to IO object
            result = loader.load(src_url, dst_io)

            assert result is None
            dst_io.seek(0)
            assert dst_io.read() == test_data

        finally:
            src_path.unlink()
