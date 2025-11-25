"""Tests for the fsspec directory implementation."""

import tempfile
from pathlib import Path, PurePosixPath
from unittest.mock import Mock, patch

import pytest
from pydantic import AnyUrl, DirectoryPath

from workstate.directory import PrefixFilter
from workstate.fsspec.directory import DirectoryLoader, DirectoryPersister


class MockPrefixFilter:
    """Mock prefix filter for testing."""

    def __init__(self, pattern: str):
        self.pattern = pattern

    def match(self, path: PurePosixPath) -> bool:
        """Simple pattern matching for testing."""
        if self.pattern.endswith("/*"):
            prefix = self.pattern[:-2]
            return str(path).startswith(prefix)
        return str(path) == self.pattern


class TestDirectoryLoaderBase:
    """Test the DirectoryLoader base functionality."""

    def test_initialization(self):
        """Test DirectoryLoader initialization."""
        loader = DirectoryLoader()
        assert loader.fs is None

    def test_initialization_with_filesystem(self):
        """Test DirectoryLoader initialization with custom filesystem."""
        mock_fs = Mock()
        loader = DirectoryLoader(fs=mock_fs)
        assert loader.fs is mock_fs


class TestDirectoryLoader:
    """Test DirectoryLoader load functionality."""

    @patch("fsspec.filesystem")
    def test_load_basic_without_filter(self, mock_fsspec_filesystem):
        """Test basic load without filter."""
        # Setup mocks
        mock_fs = Mock()
        mock_fs.find.return_value = ["data/models/model1.pkl", "data/models/model2.pkl"]

        mock_fs.isfile.side_effect = lambda x: True  # All are files
        mock_fs.open.side_effect = [
            Mock(
                __enter__=Mock(
                    return_value=Mock(read=Mock(return_value=b"model1 data"))
                ),
                __exit__=Mock(return_value=None),
            ),
            Mock(
                __enter__=Mock(
                    return_value=Mock(read=Mock(return_value=b"model2 data"))
                ),
                __exit__=Mock(return_value=None),
            ),
        ]

        mock_fsspec_filesystem.return_value = mock_fs

        loader = DirectoryLoader()

        with tempfile.TemporaryDirectory() as tmp_dir:
            dst = DirectoryPath(tmp_dir)

            loader.load(AnyUrl("s3://bucket/data/models/"), dst)

            # Verify filesystem creation
            mock_fsspec_filesystem.assert_called_once_with("s3")

            # Verify files were created
            assert (dst / "model1.pkl").exists()
            assert (dst / "model2.pkl").exists()
            assert (dst / "model1.pkl").read_bytes() == b"model1 data"
            assert (dst / "model2.pkl").read_bytes() == b"model2 data"

    @patch("fsspec.filesystem")
    def test_load_with_prefix_filter(self, mock_fsspec_filesystem):
        """Test load with prefix filter."""
        # Setup mock filesystem
        mock_fs = Mock()
        mock_fs.find.return_value = ["data/models/model1.pkl", "data/logs/error.log"]

        mock_fs.isfile.side_effect = lambda x: True
        mock_fs.open.return_value = Mock(
            __enter__=Mock(return_value=Mock(read=Mock(return_value=b"model data"))),
            __exit__=Mock(return_value=None),
        )

        mock_fsspec_filesystem.return_value = mock_fs

        loader = DirectoryLoader()
        filter_obj = MockPrefixFilter("models/*")

        with tempfile.TemporaryDirectory() as tmp_dir:
            dst = DirectoryPath(tmp_dir)

            loader.load(AnyUrl("s3://bucket/data/"), dst, filter_obj)

            # Only the filtered file should be downloaded
            assert (dst / "models" / "model1.pkl").exists()
            assert not (dst / "logs").exists()

    def test_load_with_configured_filesystem(self):
        """Test load with pre-configured filesystem."""
        # Setup mock filesystem
        mock_fs = Mock()
        mock_fs.find.return_value = ["local/file.txt"]
        mock_fs.isfile.side_effect = lambda x: True
        mock_fs.open.return_value = Mock(
            __enter__=Mock(return_value=Mock(read=Mock(return_value=b"local data"))),
            __exit__=Mock(return_value=None),
        )

        loader = DirectoryLoader(fs=mock_fs)

        with tempfile.TemporaryDirectory() as tmp_dir:
            dst = DirectoryPath(tmp_dir)

            loader.load(AnyUrl("s3://bucket/data/"), dst)

            # Should use the configured filesystem
            mock_fs.find.assert_called_once_with("s3://bucket/data/")

    def test_load_path_reference_with_filesystem(self):
        """Test load with path reference and configured filesystem."""
        mock_fs = Mock()
        mock_fs.find.return_value = ["/local/path/file.txt"]
        mock_fs.isfile.side_effect = lambda x: True
        mock_fs.open.return_value = Mock(
            __enter__=Mock(return_value=Mock(read=Mock(return_value=b"path data"))),
            __exit__=Mock(return_value=None),
        )

        loader = DirectoryLoader(fs=mock_fs)

        with tempfile.TemporaryDirectory() as tmp_dir:
            dst = DirectoryPath(tmp_dir)

            loader.load(PurePosixPath("/local/path"), dst)

            mock_fs.find.assert_called_once_with("/local/path")

    def test_load_path_reference_without_filesystem_raises_error(self):
        """Test that using path reference without filesystem raises error."""
        loader = DirectoryLoader()

        with tempfile.TemporaryDirectory() as tmp_dir:
            dst = DirectoryPath(tmp_dir)

            with pytest.raises(
                ValueError,
                match="Cannot use path reference without a configured filesystem",
            ):
                loader.load(PurePosixPath("/some/path"), dst)

    @patch("fsspec.filesystem")
    def test_load_empty_directory(self, mock_fsspec_filesystem):
        """Test loading empty directory."""
        mock_fs = Mock()
        mock_fs.find.return_value = []
        mock_fsspec_filesystem.return_value = mock_fs

        loader = DirectoryLoader()

        with tempfile.TemporaryDirectory() as tmp_dir:
            dst = DirectoryPath(tmp_dir)

            loader.load(AnyUrl("s3://bucket/empty/"), dst)

            # Directory should exist but be empty
            assert dst.exists()
            assert list(dst.iterdir()) == []

    @patch("fsspec.filesystem")
    def test_load_nonexistent_directory(self, mock_fsspec_filesystem):
        """Test loading non-existent directory."""
        mock_fs = Mock()
        mock_fs.find.side_effect = FileNotFoundError("Directory not found")
        mock_fsspec_filesystem.return_value = mock_fs

        loader = DirectoryLoader()

        with tempfile.TemporaryDirectory() as tmp_dir:
            dst = DirectoryPath(tmp_dir)

            # Should not raise error, just return without downloading
            loader.load(AnyUrl("s3://bucket/missing/"), dst)

            # Directory should exist but be empty
            assert dst.exists()
            assert list(dst.iterdir()) == []

    @patch("fsspec.filesystem")
    def test_load_creates_parent_directories(self, mock_fsspec_filesystem):
        """Test that parent directories are created for nested files."""
        mock_fs = Mock()
        mock_fs.find.return_value = ["data/deep/nested/subdirs/model.pkl"]
        mock_fs.isfile.side_effect = lambda x: True
        mock_fs.open.return_value = Mock(
            __enter__=Mock(return_value=Mock(read=Mock(return_value=b"nested data"))),
            __exit__=Mock(return_value=None),
        )
        mock_fsspec_filesystem.return_value = mock_fs

        loader = DirectoryLoader()

        with tempfile.TemporaryDirectory() as tmp_dir:
            dst = DirectoryPath(tmp_dir)

            loader.load(AnyUrl("s3://bucket/data/"), dst)

            # Verify the deeply nested file and all parent directories exist
            nested_file = dst / "deep" / "nested" / "subdirs" / "model.pkl"
            assert nested_file.exists()
            assert nested_file.read_bytes() == b"nested data"


class TestDirectoryPersisterBase:
    """Test DirectoryPersister base functionality."""

    def test_initialization(self):
        """Test DirectoryPersister initialization."""
        persister = DirectoryPersister()
        assert persister.fs is None

    def test_initialization_with_filesystem(self):
        """Test DirectoryPersister initialization with custom filesystem."""
        mock_fs = Mock()
        persister = DirectoryPersister(fs=mock_fs)
        assert persister.fs is mock_fs


class TestDirectoryPersister:
    """Test DirectoryPersister persist functionality."""

    @patch("fsspec.filesystem")
    @patch("workstate.fsspec.directory._filter_files")
    def test_persist_basic_without_filter(
        self, mock_filter_files, mock_fsspec_filesystem
    ):
        """Test basic persist without filter."""
        mock_fs = Mock()
        mock_fs.makedirs.return_value = None
        mock_fs.open.return_value = Mock(
            __enter__=Mock(return_value=Mock(write=Mock())),
            __exit__=Mock(return_value=None),
        )
        mock_fsspec_filesystem.return_value = mock_fs

        with tempfile.TemporaryDirectory() as tmp_dir:
            src = DirectoryPath(tmp_dir)

            file1 = src / "model1.pkl"
            file2 = src / "subdir" / "model2.pkl"
            file1.write_bytes(b"model1 data")
            file2.parent.mkdir()
            file2.write_bytes(b"model2 data")

            mock_filter_files.return_value = [file1, file2]

            persister = DirectoryPersister()
            persister.persist(AnyUrl("s3://bucket/data/output/"), src)

            # Verify filesystem creation
            mock_fsspec_filesystem.assert_called_once_with("s3")

    @patch("fsspec.filesystem")
    @patch("workstate.fsspec.directory._filter_files")
    def test_persist_with_filter(self, mock_filter_files, mock_fsspec_filesystem):
        """Test persist with filter."""
        mock_fs = Mock()
        mock_fs.makedirs.return_value = None
        mock_fs.open.return_value = Mock(
            __enter__=Mock(return_value=Mock(write=Mock())),
            __exit__=Mock(return_value=None),
        )
        mock_fsspec_filesystem.return_value = mock_fs

        mock_filter = Mock()

        with tempfile.TemporaryDirectory() as tmp_dir:
            src = DirectoryPath(tmp_dir)

            file1 = src / "filtered.pkl"
            file1.write_bytes(b"filtered data")

            mock_filter_files.return_value = [file1]

            persister = DirectoryPersister()
            persister.persist(AnyUrl("s3://bucket/data/models/"), src, mock_filter)

            # Verify filter was passed to _filter_files
            mock_filter_files.assert_called_once_with(src, mock_filter)

    @patch("workstate.fsspec.directory._filter_files")
    def test_persist_with_configured_filesystem(self, mock_filter_files):
        """Test persist with pre-configured filesystem."""
        mock_fs = Mock()
        mock_fs.makedirs.return_value = None
        mock_fs.open.return_value = Mock(
            __enter__=Mock(return_value=Mock(write=Mock())),
            __exit__=Mock(return_value=None),
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            src = DirectoryPath(tmp_dir)

            file1 = src / "config_test.txt"
            file1.write_bytes(b"config data")

            mock_filter_files.return_value = [file1]

            persister = DirectoryPersister(fs=mock_fs)
            persister.persist(AnyUrl("s3://bucket/config/"), src)

            # Should use the configured filesystem
            assert mock_fs.open.called

    @patch("workstate.fsspec.directory._filter_files")
    def test_persist_path_reference_with_filesystem(self, mock_filter_files):
        """Test persist with path reference and configured filesystem."""
        mock_fs = Mock()
        mock_fs.makedirs.return_value = None
        mock_fs.open.return_value = Mock(
            __enter__=Mock(return_value=Mock(write=Mock())),
            __exit__=Mock(return_value=None),
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            src = DirectoryPath(tmp_dir)

            file1 = src / "path_test.txt"
            file1.write_bytes(b"path data")

            mock_filter_files.return_value = [file1]

            persister = DirectoryPersister(fs=mock_fs)
            persister.persist(PurePosixPath("/dest/path"), src)

    def test_persist_path_reference_without_filesystem_raises_error(self):
        """Test that using path reference without filesystem raises error."""
        persister = DirectoryPersister()

        with tempfile.TemporaryDirectory() as tmp_dir:
            src = DirectoryPath(tmp_dir)

            with pytest.raises(
                ValueError,
                match="Cannot use path reference without a configured filesystem",
            ):
                persister.persist(PurePosixPath("/some/path"), src)

    @patch("fsspec.filesystem")
    @patch("workstate.fsspec.directory._filter_files")
    def test_persist_empty_prefix(self, mock_filter_files, mock_fsspec_filesystem):
        """Test persist with empty prefix (root level)."""
        mock_fs = Mock()
        mock_fs.makedirs.return_value = None
        mock_fs.open.return_value = Mock(
            __enter__=Mock(return_value=Mock(write=Mock())),
            __exit__=Mock(return_value=None),
        )
        mock_fsspec_filesystem.return_value = mock_fs

        with tempfile.TemporaryDirectory() as tmp_dir:
            src = DirectoryPath(tmp_dir)

            file1 = src / "root_file.txt"
            file1.write_bytes(b"root content")

            mock_filter_files.return_value = [file1]

            persister = DirectoryPersister()
            persister.persist(AnyUrl("s3://bucket/"), src)

            # With empty prefix, object key should just be the filename
            mock_fs.open.assert_called_with("root_file.txt", "wb")


class TestDirectoryRealFilesystem:
    """Test directory operations with real filesystem."""

    def test_load_and_persist_roundtrip_real_files(self):
        """Test loading and persisting with real files."""
        with tempfile.TemporaryDirectory() as base_dir:
            base_path = Path(base_dir)

            # Create source directory structure
            src_dir = base_path / "source"
            src_dir.mkdir()
            (src_dir / "file1.txt").write_text("Content 1")
            (src_dir / "subdir").mkdir()
            (src_dir / "subdir" / "file2.txt").write_text("Content 2")

            # Create intermediate storage directory
            storage_dir = base_path / "storage"
            storage_dir.mkdir()

            # Create destination directory
            dst_dir = base_path / "destination"
            dst_dir.mkdir()

            # Persist to storage
            persister = DirectoryPersister()
            storage_url = AnyUrl(f"file://{storage_dir}")
            persister.persist(storage_url, src_dir)

            # Load from storage
            loader = DirectoryLoader()
            loader.load(storage_url, dst_dir)

            # Verify roundtrip
            assert (dst_dir / "file1.txt").read_text() == "Content 1"
            assert (dst_dir / "subdir" / "file2.txt").read_text() == "Content 2"

    def test_load_with_prefix_filter_real_filesystem(self):
        """Test loading with prefix filter using real filesystem."""
        with tempfile.TemporaryDirectory() as base_dir:
            base_path = Path(base_dir)

            # Create source directory structure
            src_dir = base_path / "source"
            src_dir.mkdir()
            (src_dir / "models").mkdir()
            (src_dir / "models" / "model.pkl").write_text("Model data")
            (src_dir / "logs").mkdir()
            (src_dir / "logs" / "error.log").write_text("Error log")

            # Create destination directory
            dst_dir = base_path / "destination"
            dst_dir.mkdir()

            # Load with filter
            loader = DirectoryLoader()
            src_url = AnyUrl(f"file://{src_dir}")
            filter_obj = MockPrefixFilter("models/*")

            loader.load(src_url, dst_dir, filter_obj)

            # Only models should be loaded
            assert (dst_dir / "models" / "model.pkl").exists()
            assert not (dst_dir / "logs").exists()


class TestDirectoryIntegration:
    """Test integration between loader and persister."""

    def test_loader_and_persister_compatibility(self):
        """Test that loader and persister work together."""
        with tempfile.TemporaryDirectory() as base_dir:
            base_path = Path(base_dir)

            # Create source
            src_dir = base_path / "src"
            src_dir.mkdir()
            test_file = src_dir / "test.txt"
            test_content = "Integration test content"
            test_file.write_text(test_content)

            # Create intermediate storage
            storage_dir = base_path / "storage"
            storage_dir.mkdir()

            # Create destination
            dst_dir = base_path / "dst"

            # Persist then load
            persister = DirectoryPersister()
            loader = DirectoryLoader()

            storage_url = AnyUrl(f"file://{storage_dir}")

            persister.persist(storage_url, src_dir)
            loader.load(storage_url, dst_dir)

            # Verify
            result_file = dst_dir / "test.txt"
            assert result_file.exists()
            assert result_file.read_text() == test_content


class TestDirectoryErrorHandling:
    """Test error handling in directory operations."""

    def test_load_with_permission_error(self):
        """Test handling of permission errors during load."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            dst = DirectoryPath(tmp_dir)

            # Mock filesystem that raises permission error
            mock_fs = Mock()
            mock_fs.find.return_value = ["protected_file.txt"]
            mock_fs.isfile.return_value = True
            mock_fs.open.side_effect = PermissionError("Access denied")

            loader = DirectoryLoader(fs=mock_fs)

            # Should not raise, just skip the file
            loader.load(AnyUrl("file:///protected/"), dst)

    @patch("fsspec.filesystem")
    def test_persist_with_filesystem_error(self, mock_fsspec_filesystem):
        """Test handling of filesystem errors during persist."""
        mock_fs = Mock()
        mock_fs.open.side_effect = OSError("Filesystem error")
        mock_fsspec_filesystem.return_value = mock_fs

        with tempfile.TemporaryDirectory() as tmp_dir:
            src = DirectoryPath(tmp_dir)
            test_file = src / "test.txt"
            test_file.write_text("test")

            persister = DirectoryPersister()

            with pytest.raises(OSError):
                persister.persist(AnyUrl("s3://bucket/error/"), src)
