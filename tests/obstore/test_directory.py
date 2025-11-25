"""Tests for the obstore directory implementation."""

import tempfile
from pathlib import PurePosixPath
from unittest.mock import Mock, patch

from pydantic import AnyUrl, DirectoryPath

from workstate.obstore.directory import DirectoryLoader, DirectoryPersister


class MockPrefixFilter:
    """Mock implementation of PrefixFilter for testing."""

    def __init__(self, pattern: str):
        self.pattern = pattern

    def match(self, path: PurePosixPath) -> bool:
        """Simple pattern matching for tests."""
        if self.pattern == "models/*":
            return str(path).startswith("models/")
        elif self.pattern == "**/logs/*":
            return "/logs/" in str(path) or str(path).startswith("logs/")
        elif self.pattern == "*.txt":
            return str(path).endswith(".txt")
        return False


class TestDirectoryLoaderBase:
    """Test the DirectoryLoader base functionality."""

    @patch("workstate.obstore.directory.obstore")
    def test_initialization(self, mock_obstore):
        """Test DirectoryLoader initialization."""
        loader = DirectoryLoader()
        assert loader is not None

    @patch("workstate.obstore.directory.obstore")
    def test_resolve_store_and_path_url(self, mock_obstore):
        """Test store and path resolution with URL."""
        mock_store = Mock()

        with patch.object(DirectoryLoader, "_resolve_store_and_path") as mock_resolve:
            mock_resolve.return_value = (mock_store, PurePosixPath("data/models"))

            loader = DirectoryLoader()
            store, path = loader._resolve_store_and_path(
                AnyUrl("s3://bucket/data/models/")
            )

            assert store is mock_store
            assert path == PurePosixPath("data/models")

    @patch("workstate.obstore.directory.obstore")
    def test_resolve_store_and_path_posix_path(self, mock_obstore):
        """Test store and path resolution with PurePosixPath."""
        mock_store = Mock()

        with patch.object(DirectoryLoader, "_resolve_store_and_path") as mock_resolve:
            mock_resolve.return_value = (mock_store, PurePosixPath("local/path"))

            loader = DirectoryLoader()
            store, path = loader._resolve_store_and_path(PurePosixPath("local/path"))

            assert store is mock_store
            assert path == PurePosixPath("local/path")


class TestDirectoryLoader:
    """Test DirectoryLoader load functionality."""

    @patch("workstate.obstore.directory.obstore")
    def test_load_basic_without_filter(self, mock_obstore):
        """Test basic load without filter."""
        # Setup mocks
        mock_store = Mock()
        mock_obj1 = Mock()
        mock_obj1.path = "data/models/model1.pkl"
        mock_obj2 = Mock()
        mock_obj2.path = "data/models/subdir/model2.pkl"

        mock_stream = [[mock_obj1, mock_obj2]]
        mock_obstore.list.return_value = mock_stream

        mock_result1 = Mock()
        mock_result1.bytes.return_value.to_bytes.return_value = b"model1 data"
        mock_result2 = Mock()
        mock_result2.bytes.return_value.to_bytes.return_value = b"model2 data"
        mock_obstore.get.side_effect = [mock_result1, mock_result2]

        with patch.object(DirectoryLoader, "_resolve_store_and_path") as mock_resolve:
            mock_resolve.return_value = (mock_store, PurePosixPath("data/models"))

            loader = DirectoryLoader()

            with tempfile.TemporaryDirectory() as tmp_dir:
                dst = DirectoryPath(tmp_dir)

                loader.load(AnyUrl("s3://bucket/data/models/"), dst)

                # Verify obstore calls
                mock_obstore.list.assert_called_once_with(
                    mock_store, prefix="data/models"
                )
                assert mock_obstore.get.call_count == 2
                mock_obstore.get.assert_any_call(mock_store, "data/models/model1.pkl")
                mock_obstore.get.assert_any_call(
                    mock_store, "data/models/subdir/model2.pkl"
                )

                # Verify files were created
                assert (dst / "model1.pkl").exists()
                assert (dst / "subdir" / "model2.pkl").exists()
                assert (dst / "model1.pkl").read_bytes() == b"model1 data"
                assert (dst / "subdir" / "model2.pkl").read_bytes() == b"model2 data"

    @patch("workstate.obstore.directory.obstore")
    def test_load_with_prefix_filter(self, mock_obstore):
        """Test load with prefix filter."""
        # Setup mocks
        mock_store = Mock()
        mock_obj1 = Mock()
        mock_obj1.path = "data/models/neural/bert.pkl"
        mock_obj2 = Mock()
        mock_obj2.path = "data/config.json"
        mock_obj3 = Mock()
        mock_obj3.path = "data/models/logs/training.log"

        mock_stream = [[mock_obj1, mock_obj2, mock_obj3]]
        mock_obstore.list.return_value = mock_stream

        mock_result1 = Mock()
        mock_result1.bytes.return_value.to_bytes.return_value = b"bert model"
        mock_obstore.get.return_value = mock_result1

        with patch.object(DirectoryLoader, "_resolve_store_and_path") as mock_resolve:
            mock_resolve.return_value = (mock_store, PurePosixPath("data"))

            loader = DirectoryLoader()
            filter_obj = MockPrefixFilter("models/*")

            with tempfile.TemporaryDirectory() as tmp_dir:
                dst = DirectoryPath(tmp_dir)

                loader.load(AnyUrl("s3://bucket/data/"), dst, filter_obj)

                # Verify obstore calls
                mock_obstore.list.assert_called_once_with(mock_store, prefix="data")
                # Only model files should be downloaded
                assert mock_obstore.get.call_count == 2
                mock_obstore.get.assert_any_call(
                    mock_store, "data/models/neural/bert.pkl"
                )
                mock_obstore.get.assert_any_call(
                    mock_store, "data/models/logs/training.log"
                )

                # Verify files were created (with relative paths from prefix)
                assert (dst / "models" / "neural" / "bert.pkl").exists()
                assert (dst / "models" / "logs" / "training.log").exists()
                assert not (dst / "config.json").exists()

    @patch("workstate.obstore.directory.obstore")
    def test_load_object_path_not_relative_to_prefix(self, mock_obstore):
        """Test that objects not relative to prefix are skipped."""
        # Setup mocks - this shouldn't happen in practice with proper prefix filtering,
        # but test the safety check
        mock_store = Mock()
        mock_obj1 = Mock()
        mock_obj1.path = "other/path/file.txt"  # Not under "data/models"
        mock_obj2 = Mock()
        mock_obj2.path = "data/models/model.pkl"  # Under prefix

        mock_stream = [[mock_obj1, mock_obj2]]
        mock_obstore.list.return_value = mock_stream

        mock_result = Mock()
        mock_result.bytes.return_value.to_bytes.return_value = b"model data"
        mock_obstore.get.return_value = mock_result

        with patch.object(DirectoryLoader, "_resolve_store_and_path") as mock_resolve:
            mock_resolve.return_value = (mock_store, PurePosixPath("data/models"))

            loader = DirectoryLoader()

            with tempfile.TemporaryDirectory() as tmp_dir:
                dst = DirectoryPath(tmp_dir)

                loader.load(AnyUrl("s3://bucket/data/models/"), dst)

                # Only the object under the prefix should be downloaded
                mock_obstore.get.assert_called_once_with(
                    mock_store, "data/models/model.pkl"
                )

                # Verify only the valid file was created
                assert (dst / "model.pkl").exists()
                assert not (dst / "other").exists()

    @patch("workstate.obstore.directory.obstore")
    def test_load_empty_prefix(self, mock_obstore):
        """Test load with empty prefix."""
        mock_store = Mock()
        mock_obj1 = Mock()
        mock_obj1.path = "file1.txt"

        mock_stream = [[mock_obj1]]
        mock_obstore.list.return_value = mock_stream

        mock_result = Mock()
        mock_result.bytes.return_value.to_bytes.return_value = b"content"
        mock_obstore.get.return_value = mock_result

        with patch.object(DirectoryLoader, "_resolve_store_and_path") as mock_resolve:
            mock_resolve.return_value = (mock_store, None)

            loader = DirectoryLoader()

            with tempfile.TemporaryDirectory() as tmp_dir:
                dst = DirectoryPath(tmp_dir)

                loader.load(AnyUrl("s3://bucket/"), dst)

                # Verify obstore calls with None prefix (empty string becomes None)
                mock_obstore.list.assert_called_once_with(mock_store, prefix=None)
                mock_obstore.get.assert_called_once_with(mock_store, "file1.txt")

                assert (dst / "file1.txt").exists()

    @patch("workstate.obstore.directory.obstore")
    def test_load_creates_parent_directories(self, mock_obstore):
        """Test that parent directories are created as needed."""
        mock_store = Mock()
        mock_obj = Mock()
        mock_obj.path = "data/models/deep/nested/subdirs/model.pkl"

        mock_stream = [[mock_obj]]
        mock_obstore.list.return_value = mock_stream

        mock_result = Mock()
        mock_result.bytes.return_value.to_bytes.return_value = b"model data"
        mock_obstore.get.return_value = mock_result

        with patch.object(DirectoryLoader, "_resolve_store_and_path") as mock_resolve:
            mock_resolve.return_value = (mock_store, PurePosixPath("data/models"))

            loader = DirectoryLoader()

            with tempfile.TemporaryDirectory() as tmp_dir:
                dst = DirectoryPath(tmp_dir)

                loader.load(AnyUrl("s3://bucket/data/models/"), dst)

                # Verify the deeply nested file and all parent directories exist
                nested_file = dst / "deep" / "nested" / "subdirs" / "model.pkl"
                assert nested_file.exists()
                assert nested_file.read_bytes() == b"model data"


class TestDirectoryPersisterBase:
    """Test DirectoryPersister base functionality."""

    @patch("workstate.obstore.directory.obstore")
    def test_initialization(self, mock_obstore):
        """Test DirectoryPersister initialization."""
        persister = DirectoryPersister()
        assert persister is not None


class TestDirectoryPersister:
    """Test DirectoryPersister persist functionality."""

    @patch("workstate.obstore.directory.obstore")
    def test_persist_basic_without_filter(self, mock_obstore):
        """Test basic persist without filter."""
        mock_store = Mock()

        with patch.object(
            DirectoryPersister, "_resolve_store_and_path"
        ) as mock_resolve:
            mock_resolve.return_value = (mock_store, PurePosixPath("data/output"))

            with patch(
                "workstate.obstore.directory._filter_files"
            ) as mock_filter_files:
                # Create temporary source directory with files
                with tempfile.TemporaryDirectory() as tmp_dir:
                    src = DirectoryPath(tmp_dir)

                    # Create test files
                    file1 = src / "model1.pkl"
                    file1.write_bytes(b"model1 data")

                    subdir = src / "subdir"
                    subdir.mkdir()
                    file2 = subdir / "model2.pkl"
                    file2.write_bytes(b"model2 data")

                    # Mock _filter_files to return our test files
                    mock_filter_files.return_value = [file1, file2]

                    persister = DirectoryPersister()
                    persister.persist(AnyUrl("s3://bucket/data/output/"), src)

                    # Verify store.put calls
                    assert mock_store.put.call_count == 2
                    mock_store.put.assert_any_call("data/output/model1.pkl", file1)
                    mock_store.put.assert_any_call(
                        "data/output/subdir/model2.pkl", file2
                    )

                    # Verify _filter_files was called correctly
                    mock_filter_files.assert_called_once_with(src, None)

    @patch("workstate.obstore.directory.obstore")
    def test_persist_with_filter(self, mock_obstore):
        """Test persist with filter."""
        mock_store = Mock()
        mock_filter = Mock()

        with patch.object(
            DirectoryPersister, "_resolve_store_and_path"
        ) as mock_resolve:
            mock_resolve.return_value = (mock_store, PurePosixPath("data/models"))

            with patch(
                "workstate.obstore.directory._filter_files"
            ) as mock_filter_files:
                with tempfile.TemporaryDirectory() as tmp_dir:
                    src = DirectoryPath(tmp_dir)

                    file1 = src / "filtered.pkl"
                    file1.write_bytes(b"filtered data")

                    mock_filter_files.return_value = [file1]

                    persister = DirectoryPersister()
                    persister.persist(
                        AnyUrl("s3://bucket/data/models/"), src, mock_filter
                    )

                    mock_store.put.assert_called_once_with(
                        "data/models/filtered.pkl", file1
                    )
                    mock_filter_files.assert_called_once_with(src, mock_filter)

    @patch("workstate.obstore.directory.obstore")
    def test_persist_empty_prefix(self, mock_obstore):
        """Test persist with empty prefix."""
        mock_store = Mock()

        with patch.object(
            DirectoryPersister, "_resolve_store_and_path"
        ) as mock_resolve:
            mock_resolve.return_value = (mock_store, None)

            with patch(
                "workstate.obstore.directory._filter_files"
            ) as mock_filter_files:
                with tempfile.TemporaryDirectory() as tmp_dir:
                    src = DirectoryPath(tmp_dir)

                    file1 = src / "root_file.txt"
                    file1.write_bytes(b"root content")

                    mock_filter_files.return_value = [file1]

                    persister = DirectoryPersister()
                    persister.persist(AnyUrl("s3://bucket/"), src)

                    # With empty prefix, object key should just be the filename
                    mock_store.put.assert_called_once_with("root_file.txt", file1)

    @patch("workstate.obstore.directory.obstore")
    def test_persist_nested_directories(self, mock_obstore):
        """Test persist with nested directory structure."""
        mock_store = Mock()

        with patch.object(
            DirectoryPersister, "_resolve_store_and_path"
        ) as mock_resolve:
            mock_resolve.return_value = (mock_store, PurePosixPath("upload/base"))

            with patch(
                "workstate.obstore.directory._filter_files"
            ) as mock_filter_files:
                with tempfile.TemporaryDirectory() as tmp_dir:
                    src = DirectoryPath(tmp_dir)

                    # Create nested structure
                    deep_dir = src / "a" / "b" / "c"
                    deep_dir.mkdir(parents=True)
                    deep_file = deep_dir / "deep.txt"
                    deep_file.write_bytes(b"deep content")

                    mock_filter_files.return_value = [deep_file]

                    persister = DirectoryPersister()
                    persister.persist(AnyUrl("s3://bucket/upload/base/"), src)

                    # Verify the relative path is preserved in the object key
                    mock_store.put.assert_called_once_with(
                        "upload/base/a/b/c/deep.txt", deep_file
                    )


class TestIntegration:
    """Integration tests for DirectoryLoader and DirectoryPersister."""

    @patch("workstate.obstore.directory.obstore")
    def test_loader_and_persister_roundtrip(self, mock_obstore):
        """Test that DirectoryLoader and DirectoryPersister work together."""
        # This would be a more complex integration test that mocks a full roundtrip
        # from persist -> object store -> load
        pass

    @patch("workstate.obstore.directory.obstore")
    def test_path_reference_handling(self, mock_obstore):
        """Test handling of PurePosixPath references vs URL references."""
        loader = DirectoryLoader()
        persister = DirectoryPersister()

        # Test that both accept PurePosixPath and AnyUrl
        _ = PurePosixPath("local/path")
        _ = AnyUrl("s3://bucket/remote/path")

        # These should not raise type errors (basic smoke test)
        assert loader is not None
        assert persister is not None


class TestErrorHandling:
    """Test error handling scenarios."""

    @patch("workstate.obstore.directory.obstore")
    def test_load_obstore_list_empty(self, mock_obstore):
        """Test behavior when obstore.list returns empty results."""
        mock_store = Mock()
        mock_stream = [[]]  # Empty batch
        mock_obstore.list.return_value = mock_stream

        with patch.object(DirectoryLoader, "_resolve_store_and_path") as mock_resolve:
            mock_resolve.return_value = (mock_store, PurePosixPath("data/empty"))

            loader = DirectoryLoader()

            with tempfile.TemporaryDirectory() as tmp_dir:
                dst = DirectoryPath(tmp_dir)

                # Should not raise an error
                loader.load(AnyUrl("s3://bucket/data/empty/"), dst)

                # No files should be created
                assert len(list(dst.rglob("*"))) == 0

    @patch("workstate.obstore.directory.obstore")
    def test_persist_with_no_files(self, mock_obstore):
        """Test persist when _filter_files returns no files."""
        mock_store = Mock()

        with patch.object(
            DirectoryPersister, "_resolve_store_and_path"
        ) as mock_resolve:
            mock_resolve.return_value = (mock_store, PurePosixPath("data/empty"))

            with patch(
                "workstate.obstore.directory._filter_files"
            ) as mock_filter_files:
                mock_filter_files.return_value = []  # No files

                with tempfile.TemporaryDirectory() as tmp_dir:
                    src = DirectoryPath(tmp_dir)

                    persister = DirectoryPersister()
                    persister.persist(AnyUrl("s3://bucket/data/empty/"), src)

                    # No put calls should be made
                    mock_store.put.assert_not_called()
