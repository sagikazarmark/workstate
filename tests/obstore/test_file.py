"""Tests for the obstore file implementation."""

import io
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from pydantic import AnyUrl

from workstate.obstore.file import FileLoader, FilePersister


class TestFileBaseClass:
    """Test the _FileBase class functionality."""

    def test_fileloader_initialization_default(self):
        """Test FileLoader initialization with default parameters."""
        loader = FileLoader()
        assert loader.store is None
        assert loader.client_options is None

    def test_filepersister_initialization_default(self):
        """Test FilePersister initialization with default parameters."""
        persister = FilePersister()
        assert persister.store is None
        assert persister.client_options is None

    def test_fileloader_initialization_with_store(self):
        """Test FileLoader initialization with custom store."""
        mock_store = Mock()
        loader = FileLoader(store=mock_store)
        assert loader.store is mock_store
        assert loader.client_options is None

    def test_filepersister_initialization_with_store(self):
        """Test FilePersister initialization with custom store."""
        mock_store = Mock()
        persister = FilePersister(store=mock_store)
        assert persister.store is mock_store
        assert persister.client_options is None

    def test_initialization_with_client_options(self):
        """Test initialization with client options."""
        mock_options = {"some": "config"}
        loader = FileLoader(client_options=mock_options)
        assert loader.client_options is mock_options

    @patch("workstate.obstore.file.obstore.store.from_url")
    def test_resolve_store_simple_url(self, mock_from_url):
        """Test _resolve_store with a simple URL."""
        mock_store = Mock()
        mock_from_url.return_value = mock_store

        loader = FileLoader()
        url = AnyUrl("s3://bucket/path/to/file")

        store, path = loader._resolve_store(url)

        mock_from_url.assert_called_once_with("s3://bucket", client_options=None)
        assert store is mock_store
        assert path == "/path/to/file"

    @patch("workstate.obstore.file.obstore.store.from_url")
    def test_resolve_store_with_client_options(self, mock_from_url):
        """Test _resolve_store with client options."""
        mock_store = Mock()
        mock_from_url.return_value = mock_store
        mock_options = {"key": "value"}

        loader = FileLoader(client_options=mock_options)
        url = AnyUrl("s3://bucket/path/to/file")

        store, path = loader._resolve_store(url)

        mock_from_url.assert_called_once_with(
            "s3://bucket", client_options=mock_options
        )
        assert store is mock_store
        assert path == "/path/to/file"

    @patch("workstate.obstore.file.obstore.store.from_url")
    def test_resolve_store_file_url_with_host_none(self, mock_from_url):
        """Test _resolve_store with file:// URL where host is None."""
        mock_store = Mock()
        mock_from_url.return_value = mock_store

        loader = FileLoader()
        url = AnyUrl("file:///absolute/path/to/file")

        store, path = loader._resolve_store(url)

        # For file URLs with no explicit host, it should extract from path
        mock_from_url.assert_called_once()
        assert store is mock_store


class TestFileLoader:
    """Test the FileLoader implementation."""

    @patch("workstate.obstore.file.obstore.get")
    def test_load_returns_io(self, mock_get):
        """Test load method returning IO object."""
        # Mock the obstore.get response
        mock_result = Mock()
        mock_bytes = Mock()
        mock_bytes.to_bytes.return_value = b"test data"
        mock_result.bytes.return_value = mock_bytes
        mock_get.return_value = mock_result

        # Mock the store resolution
        loader = FileLoader()
        with patch.object(loader, "_resolve_store") as mock_resolve:
            mock_store = Mock()
            mock_resolve.return_value = (mock_store, "test/path")

            url = AnyUrl("s3://bucket/test/path")
            result = loader.load(url)

            assert isinstance(result, io.BytesIO)
            assert result.read() == b"test data"
            mock_get.assert_called_once_with(mock_store, "test/path")

    @patch("workstate.obstore.file.obstore.get")
    def test_load_with_path_destination(self, mock_get):
        """Test load method with Path destination."""
        # Mock the obstore.get response
        mock_result = Mock()
        mock_bytes = Mock()
        test_data = b"test file content"
        mock_bytes.to_bytes.return_value = test_data
        mock_result.bytes.return_value = mock_bytes
        mock_get.return_value = mock_result

        loader = FileLoader()
        with patch.object(loader, "_resolve_store") as mock_resolve:
            mock_store = Mock()
            mock_resolve.return_value = (mock_store, "test/path")

            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                tmp_path = Path(tmp_file.name)

            try:
                url = AnyUrl("s3://bucket/test/path")
                result = loader.load(url, tmp_path)

                assert result is None
                assert tmp_path.read_bytes() == test_data
                mock_get.assert_called_once_with(mock_store, "test/path")
            finally:
                tmp_path.unlink()

    @patch("workstate.obstore.file.obstore.get")
    def test_load_with_io_destination(self, mock_get):
        """Test load method with IO destination."""
        # Mock the obstore.get response
        mock_result = Mock()
        mock_bytes = Mock()
        test_data = b"test stream content"
        mock_bytes.to_bytes.return_value = test_data
        mock_result.bytes.return_value = mock_bytes
        mock_get.return_value = mock_result

        loader = FileLoader()
        with patch.object(loader, "_resolve_store") as mock_resolve:
            mock_store = Mock()
            mock_resolve.return_value = (mock_store, "test/path")

            dst_io = io.BytesIO()
            url = AnyUrl("s3://bucket/test/path")
            result = loader.load(url, dst_io)

            assert result is None
            dst_io.seek(0)
            assert dst_io.read() == test_data
            mock_get.assert_called_once_with(mock_store, "test/path")

    @patch("workstate.obstore.file.obstore.get")
    def test_load_with_existing_store(self, mock_get):
        """Test load method when store is already provided."""
        # Mock the obstore.get response
        mock_result = Mock()
        mock_bytes = Mock()
        mock_bytes.to_bytes.return_value = b"test data"
        mock_result.bytes.return_value = mock_bytes
        mock_get.return_value = mock_result

        mock_store = Mock()
        loader = FileLoader(store=mock_store)

        url = AnyUrl("s3://bucket/test/path")
        result = loader.load(url)

        # Should not call _resolve_store when store is already set
        mock_get.assert_called_once_with(mock_store, "s3://bucket/test/path")
        assert isinstance(result, io.BytesIO)


class TestFilePersister:
    """Test the FilePersister implementation."""

    @patch("workstate.obstore.file.obstore.put")
    def test_persist_with_bytes(self, mock_put):
        """Test persist method with bytes data."""
        persister = FilePersister()
        with patch.object(persister, "_resolve_store") as mock_resolve:
            mock_store = Mock()
            mock_resolve.return_value = (mock_store, "test/path")

            url = AnyUrl("s3://bucket/test/path")
            data = b"test data"
            persister.persist(url, data)

            mock_put.assert_called_once_with(mock_store, "test/path", data)

    @patch("workstate.obstore.file.obstore.put")
    def test_persist_with_bytearray(self, mock_put):
        """Test persist method with bytearray data."""
        persister = FilePersister()
        with patch.object(persister, "_resolve_store") as mock_resolve:
            mock_store = Mock()
            mock_resolve.return_value = (mock_store, "test/path")

            url = AnyUrl("s3://bucket/test/path")
            data = bytearray(b"test data")
            persister.persist(url, data)

            mock_put.assert_called_once_with(mock_store, "test/path", data)

    @patch("workstate.obstore.file.obstore.put")
    def test_persist_with_memoryview(self, mock_put):
        """Test persist method with memoryview data."""
        persister = FilePersister()
        with patch.object(persister, "_resolve_store") as mock_resolve:
            mock_store = Mock()
            mock_resolve.return_value = (mock_store, "test/path")

            url = AnyUrl("s3://bucket/test/path")
            original_data = b"test data"
            data = memoryview(original_data)
            persister.persist(url, data)

            mock_put.assert_called_once_with(mock_store, "test/path", data)

    @patch("workstate.obstore.file.obstore.put")
    def test_persist_with_path(self, mock_put):
        """Test persist method with Path source."""
        persister = FilePersister()
        with patch.object(persister, "_resolve_store") as mock_resolve:
            mock_store = Mock()
            mock_resolve.return_value = (mock_store, "test/path")

            url = AnyUrl("s3://bucket/test/path")
            src_path = Path("/tmp/source.txt")
            persister.persist(url, src_path)

            mock_put.assert_called_once_with(mock_store, "test/path", src_path)

    @patch("workstate.obstore.file.obstore.put")
    def test_persist_with_existing_store(self, mock_put):
        """Test persist method when store is already provided."""
        mock_store = Mock()
        persister = FilePersister(store=mock_store)

        url = AnyUrl("s3://bucket/test/path")
        data = b"test data"
        persister.persist(url, data)

        # Should not call _resolve_store when store is already set
        mock_put.assert_called_once_with(mock_store, "s3://bucket/test/path", data)


class TestIntegration:
    """Integration tests for FileLoader and FilePersister."""

    def test_loader_and_persister_compatibility(self):
        """Test that FileLoader and FilePersister work together."""
        # This is more of a structural test since we can't easily test
        # the actual obstore functionality without real backends
        loader = FileLoader()
        persister = FilePersister()

        assert hasattr(loader, "load")
        assert hasattr(persister, "persist")

        # Both should use the same base class functionality
        assert hasattr(loader, "_resolve_store")
        assert hasattr(persister, "_resolve_store")

    def test_url_handling_consistency(self):
        """Test that both classes handle URLs consistently."""
        loader = FileLoader()
        persister = FilePersister()

        url = AnyUrl("s3://test-bucket/path/to/file")

        # Both should be able to resolve the same URL format
        with patch("workstate.obstore.file.obstore.store.from_url") as mock_from_url:
            mock_store = Mock()
            mock_from_url.return_value = mock_store

            loader_result = loader._resolve_store(url)
            persister_result = persister._resolve_store(url)

            assert loader_result == persister_result
            assert mock_from_url.call_count == 2


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_path_handling(self):
        """Test handling of URLs with empty or minimal paths."""
        loader = FileLoader()

        with patch("workstate.obstore.file.obstore.store.from_url") as mock_from_url:
            mock_store = Mock()
            mock_from_url.return_value = mock_store

            # Test URL with just a scheme and host
            url = AnyUrl("s3://bucket")
            store, path = loader._resolve_store(url)

            assert store is mock_store
            # Path should be empty or minimal
            assert isinstance(path, str)

    def test_complex_path_handling(self):
        """Test handling of complex paths with special characters."""
        loader = FileLoader()

        with patch("workstate.obstore.file.obstore.store.from_url") as mock_from_url:
            mock_store = Mock()
            mock_from_url.return_value = mock_store

            # Test URL with complex path
            url = AnyUrl("s3://bucket/path/with%20spaces/and-dashes/file.txt")
            store, path = loader._resolve_store(url)

            assert store is mock_store
            assert (
                "path/with%20spaces/and-dashes/file.txt" in path
                or "/path/with%20spaces/and-dashes/file.txt" in path
            )
