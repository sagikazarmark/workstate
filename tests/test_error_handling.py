"""Tests for error handling and edge cases in workstate."""

import io
import tempfile
from pathlib import Path, PurePosixPath
from unittest.mock import Mock, patch

import pytest
from pydantic import AnyUrl, ValidationError

from workstate.obstore.file import FileLoader, FilePersister


class TestURLValidationAndParsing:
    """Test URL validation and parsing edge cases."""

    def test_invalid_url_formats(self):
        """Test handling of invalid URL formats."""
        invalid_urls = [
            "not-a-url",
            "://missing-scheme",
            "http://",
            "",
            None,
        ]

        loader = FileLoader()

        for invalid_url in invalid_urls:
            if invalid_url is None:
                continue

            with pytest.raises((ValidationError, ValueError, TypeError)):
                if invalid_url == "":
                    # Empty string might be handled differently
                    url = AnyUrl(invalid_url)
                else:
                    url = AnyUrl(invalid_url)
                    loader._resolve_store(url)

    def test_special_characters_in_urls(self):
        """Test URLs with special characters."""
        special_urls = [
            "s3://bucket/path with spaces",
            "s3://bucket/path%20with%20encoded%20spaces",
            "s3://bucket/path/with/unicode/测试",
            "s3://bucket/path/with-dashes_and_underscores",
            "s3://bucket/path.with.dots/file.txt",
        ]

        loader = FileLoader()

        with patch("workstate.obstore.file.obstore.store.from_url") as mock_from_url:
            mock_store = Mock()
            mock_from_url.return_value = mock_store

            for url_str in special_urls:
                url = AnyUrl(url_str)
                store, path = loader._resolve_store(url)

                assert store is mock_store
                assert isinstance(path, (PurePosixPath, type(None)))
                if path is not None:
                    assert len(str(path)) > 0

    def test_very_long_urls(self):
        """Test handling of very long URLs."""
        # Create a very long path
        long_path = "/".join([f"segment{i}" for i in range(100)])
        long_url = f"s3://bucket/{long_path}"

        loader = FileLoader()

        with patch("workstate.obstore.file.obstore.store.from_url") as mock_from_url:
            mock_store = Mock()
            mock_from_url.return_value = mock_store

            url = AnyUrl(long_url)
            store, path = loader._resolve_store(url)

            assert store is mock_store
            assert len(str(path)) > 900  # Should be quite long


class TestObstoreIntegrationErrors:
    """Test error handling in obstore operations."""

    @patch("workstate.obstore.file.obstore.get")
    def test_load_obstore_get_failure(self, mock_get):
        """Test handling of obstore.get failures."""
        mock_get.side_effect = Exception("Storage backend error")

        loader = FileLoader()
        with patch.object(loader, "_resolve_store") as mock_resolve:
            mock_store = Mock()
            mock_resolve.return_value = (mock_store, "test/path")

            url = AnyUrl("s3://bucket/test/path")

            with pytest.raises(Exception, match="Storage backend error"):
                loader.load(url)

    @patch("workstate.obstore.file.obstore.put")
    def test_persist_obstore_put_failure(self, mock_put):
        """Test handling of obstore.put failures."""
        mock_put.side_effect = Exception("Storage backend error")

        persister = FilePersister()
        with patch.object(persister, "_resolve_store") as mock_resolve:
            mock_store = Mock()
            mock_resolve.return_value = (mock_store, "test/path")

            url = AnyUrl("s3://bucket/test/path")
            data = b"test data"

            with pytest.raises(Exception, match="Storage backend error"):
                persister.persist(url, data)

    @patch("workstate.obstore.file.obstore.store.from_url")
    def test_store_resolution_failure(self, mock_from_url):
        """Test handling of store resolution failures."""
        mock_from_url.side_effect = ValueError("Unsupported storage scheme")

        loader = FileLoader()
        url = AnyUrl("unsupported://bucket/path")

        with pytest.raises(ValueError, match="Unsupported storage scheme"):
            loader._resolve_store(url)


class TestFileSystemErrors:
    """Test file system related errors."""

    @patch("workstate.obstore.file.obstore.get")
    def test_load_to_readonly_path(self, mock_get):
        """Test loading to a read-only destination path."""
        # Mock the obstore.get response
        mock_result = Mock()
        mock_bytes = Mock()
        mock_bytes.to_bytes.return_value = b"test data"
        mock_result.bytes.return_value = mock_bytes
        mock_get.return_value = mock_result

        loader = FileLoader()
        with patch.object(loader, "_resolve_store") as mock_resolve:
            mock_store = Mock()
            mock_resolve.return_value = (mock_store, "test/path")

            # Try to write to a path that doesn't exist and can't be created
            readonly_path = Path(
                "/root/readonly_file.txt"
            )  # This should fail on most systems
            url = AnyUrl("s3://bucket/test/path")

            # This should raise a PermissionError or similar
            with pytest.raises((PermissionError, OSError, FileNotFoundError)):
                loader.load(url, readonly_path)

    def test_persist_from_nonexistent_file(self):
        """Test persisting from a non-existent file."""
        persister = FilePersister()
        with patch.object(persister, "_resolve_store") as mock_resolve:
            mock_store = Mock()
            mock_resolve.return_value = (mock_store, "test/path")

            nonexistent_path = Path("/nonexistent/path/file.txt")
            url = AnyUrl("s3://bucket/test/path")

            # This should be handled by obstore.put, but let's test the path exists
            with patch("workstate.obstore.file.obstore.put") as mock_put:
                mock_put.side_effect = FileNotFoundError("Source file not found")

                with pytest.raises(FileNotFoundError, match="Source file not found"):
                    persister.persist(url, nonexistent_path)


class TestDataTypeEdgeCases:
    """Test edge cases with different data types."""

    @patch("workstate.obstore.file.obstore.get")
    def test_load_empty_data(self, mock_get):
        """Test loading empty data."""
        # Mock the obstore.get response with empty data
        mock_result = Mock()
        mock_bytes = Mock()
        mock_bytes.to_bytes.return_value = b""
        mock_result.bytes.return_value = mock_bytes
        mock_get.return_value = mock_result

        loader = FileLoader()
        with patch.object(loader, "_resolve_store") as mock_resolve:
            mock_store = Mock()
            mock_resolve.return_value = (mock_store, "test/path")

            url = AnyUrl("s3://bucket/test/path")
            result = loader.load(url)

            assert isinstance(result, io.BytesIO)
            assert result.read() == b""

    def test_persist_empty_data_types(self):
        """Test persisting different empty data types."""
        persister = FilePersister()
        with patch.object(persister, "_resolve_store") as mock_resolve:
            mock_store = Mock()
            mock_resolve.return_value = (mock_store, "test/path")

            url = AnyUrl("s3://bucket/test/path")

            empty_data_types = [
                b"",  # empty bytes
                bytearray(),  # empty bytearray
                memoryview(b""),  # empty memoryview
            ]

            with patch("workstate.obstore.file.obstore.put") as mock_put:
                for empty_data in empty_data_types:
                    persister.persist(url, empty_data)
                    mock_put.assert_called_with(mock_store, "test/path", empty_data)
                    mock_put.reset_mock()

    def test_persist_large_memoryview(self):
        """Test persisting a large memoryview object."""
        persister = FilePersister()
        with patch.object(persister, "_resolve_store") as mock_resolve:
            mock_store = Mock()
            mock_resolve.return_value = (mock_store, "test/path")

            # Create a large memoryview (10MB)
            large_data = b"x" * (10 * 1024 * 1024)
            large_memoryview = memoryview(large_data)

            url = AnyUrl("s3://bucket/test/path")

            with patch("workstate.obstore.file.obstore.put") as mock_put:
                persister.persist(url, large_memoryview)
                mock_put.assert_called_once_with(
                    mock_store, "test/path", large_memoryview
                )


class TestIOObjectEdgeCases:
    """Test edge cases with IO objects."""

    @patch("workstate.obstore.file.obstore.get")
    def test_load_to_closed_io(self, mock_get):
        """Test loading to a closed IO object."""
        # Mock the obstore.get response
        mock_result = Mock()
        mock_bytes = Mock()
        mock_bytes.to_bytes.return_value = b"test data"
        mock_result.bytes.return_value = mock_bytes
        mock_get.return_value = mock_result

        loader = FileLoader()
        with patch.object(loader, "_resolve_store") as mock_resolve:
            mock_store = Mock()
            mock_resolve.return_value = (mock_store, "test/path")

            # Create and close an IO object
            dst_io = io.BytesIO()
            dst_io.close()

            url = AnyUrl("s3://bucket/test/path")

            # This should raise ValueError for closed IO
            with pytest.raises(ValueError):
                loader.load(url, dst_io)

    @patch("workstate.obstore.file.obstore.get")
    def test_load_to_readonly_io(self, mock_get):
        """Test loading to a read-only IO object."""
        # Mock the obstore.get response
        mock_result = Mock()
        mock_bytes = Mock()
        mock_bytes.to_bytes.return_value = b"test data"
        mock_result.bytes.return_value = mock_bytes
        mock_get.return_value = mock_result

        loader = FileLoader()
        with patch.object(loader, "_resolve_store") as mock_resolve:
            mock_store = Mock()
            mock_resolve.return_value = (mock_store, "test/path")

            # Create a read-only IO object
            dst_io = io.BytesIO()
            dst_io = io.BufferedReader(dst_io)  # This is read-only

            url = AnyUrl("s3://bucket/test/path")

            # This should raise an error when trying to write
            with pytest.raises((AttributeError, io.UnsupportedOperation)):
                loader.load(url, dst_io)


class TestPathReferenceErrors:
    """Test error handling specific to path references."""

    def test_loader_path_without_store_error(self):
        """Test FileLoader raises error when using path without configured store."""
        loader = FileLoader()
        path_ref = PurePosixPath("/test/path.txt")

        with pytest.raises(ValueError) as exc_info:
            loader.load(path_ref)

        assert "Cannot use path reference without a configured store" in str(
            exc_info.value
        )

    def test_persister_path_without_store_error(self):
        """Test FilePersister raises error when using path without configured store."""
        persister = FilePersister()
        path_ref = PurePosixPath("/test/path.txt")
        data = b"test data"

        with pytest.raises(ValueError) as exc_info:
            persister.persist(path_ref, data)

        assert "Cannot use path reference without a configured store" in str(
            exc_info.value
        )

    def test_loader_path_with_destination_without_store_error(self):
        """Test FileLoader raises error when using path with destination but no store."""
        loader = FileLoader()
        path_ref = PurePosixPath("/test/path.txt")
        dst_path = Path("/tmp/dest.txt")

        with pytest.raises(ValueError) as exc_info:
            loader.load(path_ref, dst_path)

        assert "Cannot use path reference without a configured store" in str(
            exc_info.value
        )

    def test_loader_path_with_io_destination_without_store_error(self):
        """Test FileLoader raises error when using path with IO destination but no store."""
        loader = FileLoader()
        path_ref = PurePosixPath("/test/path.txt")
        dst_io = io.BytesIO()

        with pytest.raises(ValueError) as exc_info:
            loader.load(path_ref, dst_io)

        assert "Cannot use path reference without a configured store" in str(
            exc_info.value
        )

    def test_persister_path_with_path_source_without_store_error(self):
        """Test FilePersister raises error when using path reference with Path source but no store."""
        persister = FilePersister()
        path_ref = PurePosixPath("/test/path.txt")
        src_path = Path("/tmp/source.txt")

        with pytest.raises(ValueError) as exc_info:
            persister.persist(path_ref, src_path)

        assert "Cannot use path reference without a configured store" in str(
            exc_info.value
        )

    def test_error_message_content(self):
        """Test that error messages are descriptive and helpful."""
        loader = FileLoader()
        persister = FilePersister()
        path_ref = PurePosixPath("/test/path.txt")

        # Test loader error message
        with pytest.raises(ValueError) as loader_exc:
            loader.load(path_ref)

        loader_message = str(loader_exc.value)
        assert "path reference" in loader_message.lower()
        assert "configured store" in loader_message.lower()
        assert "URL reference" in loader_message

        # Test persister error message
        with pytest.raises(ValueError) as persister_exc:
            persister.persist(path_ref, b"data")

        persister_message = str(persister_exc.value)
        assert "path reference" in persister_message.lower()
        assert "configured store" in persister_message.lower()
        assert "URL reference" in persister_message


class TestConcurrencyAndStateSafety:
    """Test thread safety and state management."""

    def test_multiple_loaders_independence(self):
        """Test that multiple FileLoader instances are independent."""
        store1 = Mock()
        store2 = Mock()

        loader1 = FileLoader(store=store1)
        loader2 = FileLoader(store=store2)

        # Each loader should maintain its own store
        assert loader1.store is store1
        assert loader2.store is store2
        assert loader1.store is not loader2.store

    def test_multiple_persisters_independence(self):
        """Test that multiple FilePersister instances are independent."""
        store1 = Mock()
        store2 = Mock()

        persister1 = FilePersister(store=store1)
        persister2 = FilePersister(store=store2)

        # Each persister should maintain its own store
        assert persister1.store is store1
        assert persister2.store is store2
        assert persister1.store is not persister2.store

    def test_client_options_independence(self):
        """Test that client options are independent between instances."""
        options1 = {"key1": "value1"}
        options2 = {"key2": "value2"}

        loader1 = FileLoader(client_options=options1)
        loader2 = FileLoader(client_options=options2)

        # Each loader should maintain its own options
        assert loader1.client_options is options1
        assert loader2.client_options is options2
        assert loader1.client_options is not loader2.client_options

    def test_mixed_reference_types_independence(self):
        """Test that URL and path references work independently in same instance."""
        mock_store = Mock()
        loader = FileLoader(store=mock_store)
        persister = FilePersister(store=mock_store)

        # Should handle both reference types
        _ = AnyUrl("s3://bucket/url-path.txt")
        path_ref = PurePosixPath("/internal-path.txt")

        # Both reference types should be acceptable
        with patch("workstate.obstore.file.obstore.get") as mock_get:
            with patch("workstate.obstore.file.obstore.put") as mock_put:
                # Mock get response
                mock_result = Mock()
                mock_bytes = Mock()
                mock_bytes.to_bytes.return_value = b"test"
                mock_result.bytes.return_value = mock_bytes
                mock_get.return_value = mock_result

                # Test loading with path reference
                loader.load(path_ref)
                mock_get.assert_called_with(mock_store, "/internal-path.txt")

                # Test persisting with path reference
                persister.persist(path_ref, b"data")
                mock_put.assert_called_with(mock_store, "/internal-path.txt", b"data")


class TestResourceCleanup:
    """Test proper resource cleanup and memory management."""

    @patch("workstate.obstore.file.obstore.get")
    def test_io_objects_properly_created(self, mock_get):
        """Test that IO objects are properly created and can be used multiple times."""
        # Mock the obstore.get response
        mock_result = Mock()
        mock_bytes = Mock()
        test_data = b"reusable test data"
        mock_bytes.to_bytes.return_value = test_data
        mock_result.bytes.return_value = mock_bytes
        mock_get.return_value = mock_result

        loader = FileLoader()
        with patch.object(loader, "_resolve_store") as mock_resolve:
            mock_store = Mock()
            mock_resolve.return_value = (mock_store, "test/path")

            url = AnyUrl("s3://bucket/test/path")

            # Load data multiple times
            for i in range(3):
                result_io = loader.load(url)

                assert isinstance(result_io, io.BytesIO)

                # Should be able to read multiple times
                data1 = result_io.read()
                result_io.seek(0)
                data2 = result_io.read()

                assert data1 == test_data
                assert data2 == test_data
                assert data1 == data2

    def test_temporary_file_cleanup(self):
        """Test that temporary files are handled correctly."""
        _ = FileLoader()

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Create multiple temporary files
            temp_files = []
            for i in range(5):
                temp_file = tmp_path / f"temp_{i}.txt"
                temp_files.append(temp_file)

            # All files should be in the temporary directory
            for temp_file in temp_files:
                assert temp_file.parent == tmp_path

        # After the context manager, the directory should be cleaned up
        assert not tmp_path.exists()
