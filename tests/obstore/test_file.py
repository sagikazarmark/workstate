"""Tests for the obstore file implementation."""

import io
import tempfile
from pathlib import Path, PurePosixPath
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
        assert path == PurePosixPath("/path/to/file")

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
        assert path == PurePosixPath("/path/to/file")

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
    def test_load_returns_io_url(self, mock_get):
        """Test load method returning IO object with URL reference."""
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
    def test_load_returns_io_path_with_store(self, mock_get):
        """Test load method returning IO object with path reference and configured store."""
        # Mock the obstore.get response
        mock_result = Mock()
        mock_bytes = Mock()
        mock_bytes.to_bytes.return_value = b"test data"
        mock_result.bytes.return_value = mock_bytes
        mock_get.return_value = mock_result

        # Create loader with configured store
        mock_store = Mock()
        loader = FileLoader(store=mock_store)
        path_ref = PurePosixPath("/test/path.txt")

        result = loader.load(path_ref)

        assert isinstance(result, io.BytesIO)
        assert result.read() == b"test data"
        mock_get.assert_called_once_with(mock_store, "/test/path.txt")

    def test_load_returns_io_path_without_store_raises_error(self):
        """Test load method with path reference but no store raises error."""
        loader = FileLoader()
        path_ref = PurePosixPath("/test/path.txt")

        with pytest.raises(ValueError) as exc_info:
            loader.load(path_ref)

        assert "Cannot use path reference without a configured store" in str(
            exc_info.value
        )

    @patch("workstate.obstore.file.obstore.get")
    def test_load_with_path_destination_url(self, mock_get):
        """Test load method with Path destination and URL reference."""
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
    def test_load_with_path_destination_path_with_store(self, mock_get):
        """Test load method with Path destination and path reference using configured store."""
        # Mock the obstore.get response
        mock_result = Mock()
        mock_bytes = Mock()
        test_data = b"test file content"
        mock_bytes.to_bytes.return_value = test_data
        mock_result.bytes.return_value = mock_bytes
        mock_get.return_value = mock_result

        # Create loader with configured store
        mock_store = Mock()
        loader = FileLoader(store=mock_store)

        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_path = Path(tmp_file.name)

        try:
            path_ref = PurePosixPath("/test/path.txt")
            result = loader.load(path_ref, tmp_path)

            assert result is None
            assert tmp_path.read_bytes() == test_data
            mock_get.assert_called_once_with(mock_store, "/test/path.txt")
        finally:
            tmp_path.unlink()

    def test_load_with_path_destination_path_without_store_raises_error(self):
        """Test load method with Path destination and path reference but no store raises error."""
        loader = FileLoader()
        path_ref = PurePosixPath("/test/path.txt")
        tmp_path = Path("/tmp/test.txt")

        with pytest.raises(ValueError) as exc_info:
            loader.load(path_ref, tmp_path)

        assert "Cannot use path reference without a configured store" in str(
            exc_info.value
        )

    @patch("workstate.obstore.file.obstore.get")
    def test_load_with_io_destination_url(self, mock_get):
        """Test load method with IO destination and URL reference."""
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
        mock_get.assert_called_once_with(mock_store, "/test/path")
        assert isinstance(result, io.BytesIO)


class TestFilePersister:
    """Test the FilePersister implementation."""

    @patch("workstate.obstore.file.obstore.put")
    def test_persist_with_bytes_data_url(self, mock_put):
        """Test persist method with bytes data and URL reference."""
        persister = FilePersister()
        with patch.object(persister, "_resolve_store") as mock_resolve:
            mock_store = Mock()
            mock_resolve.return_value = (mock_store, "test/path")

            url = AnyUrl("s3://bucket/test/path")
            data = b"test data"
            persister.persist(url, data)

            mock_put.assert_called_once_with(mock_store, "test/path", data)

    @patch("workstate.obstore.file.obstore.put")
    def test_persist_with_bytes_data_path_with_store(self, mock_put):
        """Test persist method with bytes data and path reference using configured store."""
        # Create persister with configured store
        mock_store = Mock()
        persister = FilePersister(store=mock_store)
        path_ref = PurePosixPath("/test/path.txt")
        data = b"test data"

        persister.persist(path_ref, data)

        mock_put.assert_called_once_with(mock_store, "/test/path.txt", data)

    def test_persist_with_bytes_data_path_without_store_raises_error(self):
        """Test persist method with bytes data and path reference but no store raises error."""
        persister = FilePersister()
        path_ref = PurePosixPath("/test/path.txt")
        data = b"test data"

        with pytest.raises(ValueError) as exc_info:
            persister.persist(path_ref, data)

        assert "Cannot use path reference without a configured store" in str(
            exc_info.value
        )

    @patch("workstate.obstore.file.obstore.put")
    def test_persist_with_bytearray_data_url(self, mock_put):
        """Test persist method with bytearray data and URL reference."""
        persister = FilePersister()
        with patch.object(persister, "_resolve_store") as mock_resolve:
            mock_store = Mock()
            mock_resolve.return_value = (mock_store, "test/path")

            url = AnyUrl("s3://bucket/test/path")
            data = bytearray(b"test data")
            persister.persist(url, data)

            mock_put.assert_called_once_with(mock_store, "test/path", data)

    @patch("workstate.obstore.file.obstore.put")
    def test_persist_with_bytearray_data_path_with_store(self, mock_put):
        """Test persist method with bytearray data and path reference using configured store."""
        # Create persister with configured store
        mock_store = Mock()
        persister = FilePersister(store=mock_store)
        path_ref = PurePosixPath("/test/path.txt")
        data = bytearray(b"test data")

        persister.persist(path_ref, data)

        mock_put.assert_called_once_with(mock_store, "/test/path.txt", data)

    @patch("workstate.obstore.file.obstore.put")
    def test_persist_with_memoryview_data_url(self, mock_put):
        """Test persist method with memoryview data and URL reference."""
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
    def test_persist_with_memoryview_data_path_with_store(self, mock_put):
        """Test persist method with memoryview data and path reference using configured store."""
        # Create persister with configured store
        mock_store = Mock()
        persister = FilePersister(store=mock_store)
        path_ref = PurePosixPath("/test/path.txt")
        original_data = b"test data"
        data = memoryview(original_data)

        persister.persist(path_ref, data)

        mock_put.assert_called_once_with(mock_store, "/test/path.txt", data)

    @patch("workstate.obstore.file.obstore.put")
    def test_persist_with_path_source_url(self, mock_put):
        """Test persist method with Path source and URL reference."""
        persister = FilePersister()
        with patch.object(persister, "_resolve_store") as mock_resolve:
            mock_store = Mock()
            mock_resolve.return_value = (mock_store, "test/path")

            url = AnyUrl("s3://bucket/test/path")
            src_path = Path("/tmp/source.txt")
            persister.persist(url, src_path)

            mock_put.assert_called_once_with(mock_store, "test/path", src_path)

    @patch("workstate.obstore.file.obstore.put")
    def test_persist_with_path_source_path_ref_with_store(self, mock_put):
        """Test persist method with Path source and path reference using configured store."""
        # Create persister with configured store
        mock_store = Mock()
        persister = FilePersister(store=mock_store)
        path_ref = PurePosixPath("/test/path.txt")
        src_path = Path("/tmp/source.txt")

        persister.persist(path_ref, src_path)

        mock_put.assert_called_once_with(mock_store, "/test/path.txt", src_path)

    def test_persist_with_path_source_path_ref_without_store_raises_error(self):
        """Test persist method with Path source and path reference but no store raises error."""
        persister = FilePersister()
        path_ref = PurePosixPath("/test/path.txt")
        src_path = Path("/tmp/source.txt")

        with pytest.raises(ValueError) as exc_info:
            persister.persist(path_ref, src_path)

        assert "Cannot use path reference without a configured store" in str(
            exc_info.value
        )

    @patch("workstate.obstore.file.obstore.put")
    def test_persist_with_existing_store(self, mock_put):
        """Test persist method when store is already provided."""
        mock_store = Mock()
        persister = FilePersister(store=mock_store)

        url = AnyUrl("s3://bucket/test/path")
        data = b"test data"
        persister.persist(url, data)

        # Should not call _resolve_store when store is already set
        mock_put.assert_called_once_with(mock_store, "/test/path", data)


class TestIntegration:
    """Test integration scenarios with obstore."""

    @patch("workstate.obstore.file.obstore.get")
    @patch("workstate.obstore.file.obstore.put")
    def test_loader_and_persister_with_shared_store(self, mock_put, mock_get):
        """Test that FileLoader and FilePersister work together with shared store."""
        # Mock the obstore.get response
        mock_result = Mock()
        mock_bytes = Mock()
        test_data = b"shared store test data"
        mock_bytes.to_bytes.return_value = test_data
        mock_result.bytes.return_value = mock_bytes
        mock_get.return_value = mock_result

        # Create shared store
        mock_store = Mock()
        loader = FileLoader(store=mock_store)
        persister = FilePersister(store=mock_store)

        # Test with path references
        source_path = PurePosixPath("/source/data.txt")
        dest_path = PurePosixPath("/dest/data.txt")

        # Load data
        loaded_io = loader.load(source_path)
        loaded_data = loaded_io.read()

        # Persist data
        persister.persist(dest_path, loaded_data)

        # Verify operations
        mock_get.assert_called_once_with(mock_store, "/source/data.txt")
        mock_put.assert_called_once_with(mock_store, "/dest/data.txt", test_data)

    @patch("workstate.obstore.file.obstore.get")
    def test_load_with_io_destination_path_with_store(self, mock_get):
        """Test load method with IO destination and path reference using configured store."""
        # Mock the obstore.get response
        mock_result = Mock()
        mock_bytes = Mock()
        test_data = b"test stream content"
        mock_bytes.to_bytes.return_value = test_data
        mock_result.bytes.return_value = mock_bytes
        mock_get.return_value = mock_result

        # Create loader with configured store
        mock_store = Mock()
        loader = FileLoader(store=mock_store)
        path_ref = PurePosixPath("/test/file.txt")
        dst_io = io.BytesIO()

        result = loader.load(path_ref, dst_io)

        assert result is None
        dst_io.seek(0)
        assert dst_io.read() == test_data
        mock_get.assert_called_once_with(mock_store, "/test/file.txt")

    def test_load_with_io_destination_path_without_store_raises_error(self):
        """Test load method with IO destination and path reference but no store raises error."""
        loader = FileLoader()
        path_ref = PurePosixPath("/test/file.txt")
        dst_io = io.BytesIO()

        with pytest.raises(ValueError) as exc_info:
            loader.load(path_ref, dst_io)

        assert "Cannot use path reference without a configured store" in str(
            exc_info.value
        )


class TestErrorHandling:
    """Test error handling scenarios specific to obstore."""

    def test_loader_path_reference_without_store(self):
        """Test that using path reference without store raises appropriate error."""
        loader = FileLoader()
        path_ref = PurePosixPath("/test/path.txt")

        with pytest.raises(ValueError) as exc_info:
            loader.load(path_ref)

        assert "Cannot use path reference without a configured store" in str(
            exc_info.value
        )

    def test_persister_path_reference_without_store(self):
        """Test that using path reference without store raises appropriate error."""
        persister = FilePersister()
        path_ref = PurePosixPath("/test/path.txt")
        data = b"test data"

        with pytest.raises(ValueError) as exc_info:
            persister.persist(path_ref, data)

        assert "Cannot use path reference without a configured store" in str(
            exc_info.value
        )

    def test_mixed_references_same_operation(self):
        """Test that mixed URL and path references work in same workflow."""
        mock_store = Mock()
        loader = FileLoader(store=mock_store)
        persister = FilePersister(store=mock_store)

        # Should accept both URL and path references
        with patch("workstate.obstore.file.obstore.get") as mock_get:
            with patch("workstate.obstore.file.obstore.put") as mock_put:
                # Mock get response
                mock_result = Mock()
                mock_bytes = Mock()
                mock_bytes.to_bytes.return_value = b"test"
                mock_result.bytes.return_value = mock_bytes
                mock_get.return_value = mock_result

                # Load from path reference
                path_ref = PurePosixPath("/internal/data.txt")
                _ = loader.load(path_ref)

                # Persist with URL reference (should resolve store)
                with patch.object(persister, "_resolve_store_and_path") as mock_resolve:
                    mock_resolve.return_value = (
                        mock_store,
                        PurePosixPath("external/backup.txt"),
                    )
                    url_ref = AnyUrl("s3://bucket/external/backup.txt")
                    persister.persist(url_ref, b"test")

                    # Verify both operations
                    mock_get.assert_called_once_with(mock_store, "/internal/data.txt")
                    mock_put.assert_called_once_with(
                        mock_store, "external/backup.txt", b"test"
                    )


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
            # Path should be None for empty paths
            assert path is None

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
            assert "path/with%20spaces/and-dashes/file.txt" in str(
                path
            ) or "/path/with%20spaces/and-dashes/file.txt" in str(path)
