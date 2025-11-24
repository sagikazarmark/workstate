"""Performance and benchmark tests for workstate."""

import io
import time
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from pydantic import AnyUrl

from workstate.obstore.file import FileLoader, FilePersister


class TestPerformanceBasics:
    """Basic performance tests for workstate operations."""

    @pytest.mark.slow
    @patch("workstate.obstore.file.obstore.get")
    def test_load_performance_small_files(self, mock_get):
        """Test load performance with small files."""
        # Mock the obstore.get response
        mock_result = Mock()
        mock_bytes = Mock()
        test_data = b"small test data" * 100  # ~1.5KB
        mock_bytes.to_bytes.return_value = test_data
        mock_result.bytes.return_value = mock_bytes
        mock_get.return_value = mock_result

        loader = FileLoader()
        with patch.object(loader, "_resolve_store") as mock_resolve:
            mock_store = Mock()
            mock_resolve.return_value = (mock_store, "test/path")

            url = AnyUrl("s3://bucket/test/path")

            # Measure time for multiple loads
            start_time = time.time()
            num_operations = 100

            for _ in range(num_operations):
                result = loader.load(url)
                data = result.read()
                assert len(data) == len(test_data)

            end_time = time.time()
            total_time = end_time - start_time
            avg_time_per_op = total_time / num_operations

            # Should be able to handle small files quickly
            # Allow up to 10ms per operation (generous threshold)
            assert avg_time_per_op < 0.01, (
                f"Average time per operation: {avg_time_per_op:.4f}s"
            )

    @pytest.mark.slow
    @patch("workstate.obstore.file.obstore.put")
    def test_persist_performance_small_files(self, mock_put):
        """Test persist performance with small files."""
        persister = FilePersister()
        with patch.object(persister, "_resolve_store") as mock_resolve:
            mock_store = Mock()
            mock_resolve.return_value = (mock_store, "test/path")

            url = AnyUrl("s3://bucket/test/path")
            test_data = b"small test data" * 100  # ~1.5KB

            # Measure time for multiple persists
            start_time = time.time()
            num_operations = 100

            for _ in range(num_operations):
                persister.persist(url, test_data)

            end_time = time.time()
            total_time = end_time - start_time
            avg_time_per_op = total_time / num_operations

            # Should be able to handle small files quickly
            assert avg_time_per_op < 0.01, (
                f"Average time per operation: {avg_time_per_op:.4f}s"
            )
            assert mock_put.call_count == num_operations

    @pytest.mark.slow
    @patch("workstate.obstore.file.obstore.get")
    def test_load_performance_medium_files(self, mock_get):
        """Test load performance with medium-sized files."""
        # Mock the obstore.get response
        mock_result = Mock()
        mock_bytes = Mock()
        test_data = b"x" * (1024 * 1024)  # 1MB
        mock_bytes.to_bytes.return_value = test_data
        mock_result.bytes.return_value = mock_bytes
        mock_get.return_value = mock_result

        loader = FileLoader()
        with patch.object(loader, "_resolve_store") as mock_resolve:
            mock_store = Mock()
            mock_resolve.return_value = (mock_store, "test/path")

            url = AnyUrl("s3://bucket/test/path")

            # Measure time for fewer operations with larger data
            start_time = time.time()
            num_operations = 10

            for _ in range(num_operations):
                result = loader.load(url)
                data = result.read()
                assert len(data) == len(test_data)

            end_time = time.time()
            total_time = end_time - start_time
            avg_time_per_op = total_time / num_operations

            # Should handle 1MB files reasonably quickly
            # Allow up to 100ms per operation
            assert avg_time_per_op < 0.1, (
                f"Average time per operation: {avg_time_per_op:.4f}s"
            )

    def test_memory_usage_io_objects(self):
        """Test memory efficiency of IO object creation."""
        loader = FileLoader()

        # Create many IO objects and verify they don't accumulate
        ios = []
        for i in range(1000):
            # Simulate returning IO objects
            test_io = io.BytesIO(f"test data {i}".encode())
            ios.append(test_io)

        # All IO objects should be independent
        for i, test_io in enumerate(ios):
            test_io.seek(0)
            data = test_io.read()
            expected = f"test data {i}".encode()
            assert data == expected

        # Cleanup should work properly
        for test_io in ios:
            test_io.close()


class TestScalabilityPatterns:
    """Test scalability patterns and bulk operations."""

    @patch("workstate.obstore.file.obstore.store.from_url")
    def test_store_resolution_caching_pattern(self, mock_from_url):
        """Test pattern for efficient store reuse."""
        mock_store = Mock()
        mock_from_url.return_value = mock_store

        # Pattern 1: Create store once, reuse for multiple operations
        shared_store = mock_store
        loader = FileLoader(store=shared_store)
        persister = FilePersister(store=shared_store)

        # Multiple operations should not call from_url
        urls = [AnyUrl(f"s3://bucket/file{i}") for i in range(10)]

        with (
            patch("workstate.obstore.file.obstore.get") as mock_get,
            patch("workstate.obstore.file.obstore.put") as mock_put,
        ):
            # Setup mocks
            mock_result = Mock()
            mock_bytes = Mock()
            mock_bytes.to_bytes.return_value = b"test"
            mock_result.bytes.return_value = mock_bytes
            mock_get.return_value = mock_result

            # Perform multiple operations
            for url in urls:
                loader.load(url)
                persister.persist(url, b"test")

        # Should not have called from_url since store was provided
        mock_from_url.assert_not_called()
        assert mock_get.call_count == len(urls)
        assert mock_put.call_count == len(urls)

    def test_bulk_url_processing(self):
        """Test processing multiple URLs efficiently."""
        loader = FileLoader()

        # Test URL parsing efficiency
        base_urls = [
            "s3://bucket",
            "gs://bucket",
            "azure://container",
            "file:///tmp",
        ]

        all_urls = []
        for base in base_urls:
            for i in range(25):  # 25 files per scheme = 100 total
                all_urls.append(AnyUrl(f"{base}/file{i}.txt"))

        with patch("workstate.obstore.file.obstore.store.from_url") as mock_from_url:
            mock_store = Mock()
            mock_from_url.return_value = mock_store

            start_time = time.time()

            # Process all URLs
            for url in all_urls:
                store, path = loader._resolve_store(url)
                assert store is mock_store
                assert isinstance(path, str)

            end_time = time.time()
            total_time = end_time - start_time

            # Should process URLs efficiently
            # Allow 1ms per URL on average
            assert total_time < 0.1, f"URL processing took too long: {total_time:.4f}s"

    @pytest.mark.slow
    def test_concurrent_instance_performance(self):
        """Test performance with multiple concurrent instances."""
        # Create multiple instances
        num_instances = 50
        loaders = [FileLoader() for _ in range(num_instances)]
        persisters = [FilePersister() for _ in range(num_instances)]

        # Test that creation is fast
        start_time = time.time()
        for i in range(num_instances):
            loader = FileLoader()
            persister = FilePersister()
            # Basic operations should be fast
            assert hasattr(loader, "load")
            assert hasattr(persister, "persist")
        end_time = time.time()

        creation_time = end_time - start_time
        # Should create instances quickly
        assert creation_time < 0.1, (
            f"Instance creation took too long: {creation_time:.4f}s"
        )


class TestMemoryEfficiency:
    """Test memory efficiency patterns."""

    @patch("workstate.obstore.file.obstore.get")
    def test_streaming_like_behavior(self, mock_get):
        """Test that IO objects can be used in streaming fashion."""
        # Mock the obstore.get response
        mock_result = Mock()
        mock_bytes = Mock()
        large_data = b"x" * (1024 * 1024)  # 1MB
        mock_bytes.to_bytes.return_value = large_data
        mock_result.bytes.return_value = mock_bytes
        mock_get.return_value = mock_result

        loader = FileLoader()
        with patch.object(loader, "_resolve_store") as mock_resolve:
            mock_store = Mock()
            mock_resolve.return_value = (mock_store, "test/path")

            url = AnyUrl("s3://bucket/large-file")
            result_io = loader.load(url)

            # Should be able to read in chunks
            chunk_size = 1024
            total_read = 0
            while True:
                chunk = result_io.read(chunk_size)
                if not chunk:
                    break
                total_read += len(chunk)
                assert len(chunk) <= chunk_size

            assert total_read == len(large_data)

    def test_memoryview_efficiency(self):
        """Test that memoryview objects are handled efficiently."""
        persister = FilePersister()
        with patch.object(persister, "_resolve_store") as mock_resolve:
            mock_store = Mock()
            mock_resolve.return_value = (mock_store, "test/path")

            # Create a large data buffer
            large_buffer = bytearray(1024 * 1024)  # 1MB
            # Create memoryview slices
            views = [
                memoryview(large_buffer)[0:1024],  # First KB
                memoryview(large_buffer)[1024:2048],  # Second KB
                memoryview(large_buffer)[-1024:],  # Last KB
            ]

            url = AnyUrl("s3://bucket/test")

            with patch("workstate.obstore.file.obstore.put") as mock_put:
                start_time = time.time()

                for view in views:
                    persister.persist(url, view)

                end_time = time.time()
                operation_time = end_time - start_time

                # Operations should be efficient with memoryviews
                assert operation_time < 0.01, (
                    f"Memoryview operations took too long: {operation_time:.4f}s"
                )
                assert mock_put.call_count == len(views)


class TestResourceUtilization:
    """Test resource utilization patterns."""

    def test_minimal_object_creation(self):
        """Test that objects are created with minimal overhead."""
        # Measure basic object creation
        start_time = time.time()

        for _ in range(1000):
            loader = FileLoader()
            persister = FilePersister()
            # Objects should be lightweight
            assert loader.store is None
            assert loader.client_options is None
            assert persister.store is None
            assert persister.client_options is None

        end_time = time.time()
        creation_time = end_time - start_time

        # Should create objects very quickly
        assert creation_time < 0.1, (
            f"Object creation took too long: {creation_time:.4f}s"
        )

    def test_configuration_reuse_efficiency(self):
        """Test efficiency of configuration reuse."""
        # Shared configuration
        mock_store = Mock()
        client_options = {"region": "us-west-2", "key": "value"}

        # Create multiple instances with same config
        start_time = time.time()

        instances = []
        for _ in range(100):
            loader = FileLoader(store=mock_store, client_options=client_options)
            persister = FilePersister(store=mock_store, client_options=client_options)
            instances.append((loader, persister))

        end_time = time.time()
        creation_time = end_time - start_time

        # Should reuse configurations efficiently
        assert creation_time < 0.05, (
            f"Configuration reuse took too long: {creation_time:.4f}s"
        )

        # Verify all instances share the same references
        for loader, persister in instances:
            assert loader.store is mock_store
            assert loader.client_options is client_options
            assert persister.store is mock_store
            assert persister.client_options is client_options
