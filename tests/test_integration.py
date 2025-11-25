"""Integration tests for workstate functionality with obstore backend."""

import io
import tempfile
from pathlib import Path, PurePosixPath
from unittest.mock import Mock, patch

import pytest
from pydantic import AnyUrl

# These tests require the obstore optional dependency
pytest.importorskip("obstore")

from workstate.obstore.file import FileLoader, FilePersister


class TestObstoreIntegration:
    """Integration tests that require obstore to be available."""

    @pytest.mark.integration
    @patch("obstore.store.from_url")
    @patch("workstate.obstore.file.obstore")
    def test_full_roundtrip_with_bytes(self, mock_obstore, mock_from_url):
        """Test full roundtrip: persist data and then load it back."""
        # Setup mocks for persist operation
        mock_store = Mock()
        mock_from_url.return_value = mock_store

        # Setup mocks for load operation
        mock_result = Mock()
        mock_bytes_result = Mock()
        test_data = b"integration test data"
        mock_bytes_result.to_bytes.return_value = test_data
        mock_result.bytes.return_value = mock_bytes_result
        mock_obstore.get.return_value = mock_result

        # Test the roundtrip
        url = AnyUrl("s3://test-bucket/integration-test-file")

        # Persist the data
        persister = FilePersister()
        persister.persist(url, test_data)

        # Load the data back
        loader = FileLoader()
        loaded_io = loader.load(url)

        # Verify the roundtrip worked
        assert isinstance(loaded_io, io.BytesIO)
        loaded_data = loaded_io.read()
        assert loaded_data == test_data

        # Verify the underlying calls
        mock_from_url.assert_called()
        mock_obstore.put.assert_called_once_with(
            mock_store, "/integration-test-file", test_data
        )
        mock_obstore.get.assert_called_once_with(mock_store, "/integration-test-file")

    @pytest.mark.integration
    @patch("obstore.store.from_url")
    @patch("workstate.obstore.file.obstore")
    def test_full_roundtrip_with_files(self, mock_obstore, mock_from_url):
        """Test full roundtrip using file paths."""
        # Setup mocks
        mock_store = Mock()
        mock_from_url.return_value = mock_store

        # Setup mocks for load operation
        mock_result = Mock()
        mock_bytes_result = Mock()
        test_data = b"file integration test data"
        mock_bytes_result.to_bytes.return_value = test_data
        mock_result.bytes.return_value = mock_bytes_result
        mock_obstore.get.return_value = mock_result

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Create source file
            src_file = tmp_path / "source.txt"
            src_file.write_bytes(test_data)

            # Create destination file path
            dst_file = tmp_path / "destination.txt"

            url = AnyUrl("s3://test-bucket/file-integration-test")

            # Persist from file
            persister = FilePersister()
            persister.persist(url, src_file)

            # Load to file
            loader = FileLoader()
            result = loader.load(url, dst_file)

            # Verify the roundtrip worked
            assert result is None  # load() with destination should return None
            assert dst_file.exists()
            assert dst_file.read_bytes() == test_data

            # Verify the underlying calls
            mock_from_url.assert_called()
            mock_obstore.put.assert_called_once_with(
                mock_store, "/file-integration-test", src_file
            )
            mock_obstore.get.assert_called_once_with(
                mock_store, "/file-integration-test"
            )

    @pytest.mark.integration
    @patch("obstore.store.from_url")
    @patch("workstate.obstore.file.obstore")
    def test_shared_store_instance(self, mock_obstore, mock_from_url):
        """Test that multiple instances can share the same store."""
        # Setup mocks
        mock_store = Mock()

        # Setup mocks for operations
        mock_result = Mock()
        mock_bytes_result = Mock()
        test_data = b"shared store test"
        mock_bytes_result.to_bytes.return_value = test_data
        mock_result.bytes.return_value = mock_bytes_result
        mock_obstore.get.return_value = mock_result

        # Create instances with shared store
        loader = FileLoader(store=mock_store)
        persister = FilePersister(store=mock_store)

        url = AnyUrl("s3://test-bucket/shared-store-test")

        # Persist and load with shared store
        persister.persist(url, test_data)
        loaded_io = loader.load(url)

        # Verify data integrity
        assert isinstance(loaded_io, io.BytesIO)
        loaded_data = loaded_io.read()
        assert loaded_data == test_data

        # Verify that store resolution was not called (since store was provided)
        mock_from_url.assert_not_called()

        # Verify operations used the shared store
        mock_obstore.put.assert_called_once_with(
            mock_store, "/shared-store-test", test_data
        )
        mock_obstore.get.assert_called_once_with(mock_store, "/shared-store-test")

    @pytest.mark.integration
    def test_protocol_compliance(self):
        """Test that obstore implementations comply with the protocols."""
        from workstate.file import FileLoader as FileLoaderProtocol
        from workstate.file import FilePersister as FilePersisterProtocol
        from workstate.obstore.file import FileLoader, FilePersister

        # Test that instances can be used where protocols are expected
        loader = FileLoader()
        persister = FilePersister()

        # These should work without type errors (in a real type checker)
        def use_loader(fl: FileLoaderProtocol):
            return hasattr(fl, "load")

        def use_persister(fp: FilePersisterProtocol):
            return hasattr(fp, "persist")

        assert use_loader(loader)
        assert use_persister(persister)

    @pytest.mark.integration
    @patch("obstore.store.from_url")
    @patch("workstate.obstore.file.obstore")
    def test_different_url_schemes(self, mock_obstore, mock_from_url):
        """Test that different URL schemes are handled correctly."""
        mock_store = Mock()
        mock_from_url.return_value = mock_store

        loader = FileLoader()

        test_urls = [
            "s3://bucket/key",
            "gs://bucket/key",
            "azure://container/key",
            "file:///local/path",
        ]

        for url_str in test_urls:
            url = AnyUrl(url_str)
            store, path = loader._resolve_store(url)

            assert store is mock_store
            assert isinstance(path, (PurePosixPath, type(None)))
            if path is not None:
                assert len(str(path)) > 0

    @pytest.mark.integration
    @patch("obstore.store.from_url")
    @patch("workstate.obstore.file.obstore")
    def test_client_options_propagation(self, mock_obstore, mock_from_url):
        """Test that client options are properly propagated."""
        mock_store = Mock()
        mock_from_url.return_value = mock_store

        client_options = {
            "aws_access_key_id": "test_key",
            "aws_secret_access_key": "test_secret",
            "region": "us-west-2",
        }

        loader = FileLoader(client_options=client_options)

        url = AnyUrl("s3://test-bucket/test-key")

        store, path = loader._resolve_store(url)

        # Verify that client options were passed to from_url
        mock_from_url.assert_called_once_with(
            "s3://test-bucket", client_options=client_options
        )
        assert store is mock_store
        assert path == PurePosixPath("/test-key")

    @pytest.mark.integration
    @patch("obstore.store.from_url")
    @patch("workstate.obstore.file.obstore")
    def test_error_handling(self, mock_obstore, mock_from_url):
        """Test error handling in obstore operations."""
        # Test exception propagation from obstore
        mock_from_url.side_effect = ValueError("Invalid URL")

        loader = FileLoader()
        url = AnyUrl("invalid://url/format")

        with pytest.raises(ValueError, match="Invalid URL"):
            loader._resolve_store(url)

    @pytest.mark.integration
    @patch("obstore.store.from_url")
    @patch("workstate.obstore.file.obstore")
    def test_memory_efficiency_large_data(self, mock_obstore, mock_from_url):
        """Test handling of larger data to verify memory efficiency."""
        mock_store = Mock()
        mock_from_url.return_value = mock_store

        # Create relatively large test data (1MB)
        large_data = b"x" * (1024 * 1024)

        # Setup mocks for load operation
        mock_result = Mock()
        mock_bytes_result = Mock()
        mock_bytes_result.to_bytes.return_value = large_data
        mock_result.bytes.return_value = mock_bytes_result
        mock_obstore.get.return_value = mock_result

        url = AnyUrl("s3://test-bucket/large-file")

        # Test persist and load with large data
        persister = FilePersister()
        persister.persist(url, large_data)

        loader = FileLoader()
        loaded_io = loader.load(url)

        # Verify data integrity
        assert isinstance(loaded_io, io.BytesIO)
        loaded_data = loaded_io.read()
        assert len(loaded_data) == len(large_data)
        assert loaded_data == large_data

        # Verify operations were called correctly
        mock_obstore.put.assert_called_once_with(mock_store, "/large-file", large_data)
        mock_obstore.get.assert_called_once_with(mock_store, "/large-file")
