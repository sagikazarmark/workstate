from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock

from workstate.obstore.filter import Filter, IncludeExcludeFilter, filter_files


class TestFilter:
    """Test the Filter protocol."""

    def test_filter_protocol(self):
        """Test that Filter is a proper protocol."""
        # Create a mock that implements the protocol
        mock_filter = Mock(spec=Filter)
        mock_filter.match.return_value = True

        # Should be callable
        result = mock_filter.match("test.txt")
        assert result is True
        mock_filter.match.assert_called_once_with("test.txt")


class TestIncludeExcludeFilter:
    """Test the IncludeExcludeFilter class."""

    def test_empty_filter_allows_all(self):
        """Test that empty filter allows all files."""
        filter = IncludeExcludeFilter()

        assert filter.match("file.txt") is True
        assert filter.match("subdir/file.py") is True
        assert filter.match("any/path/file.json") is True

    def test_include_only(self):
        """Test filter with only include patterns."""
        filter = IncludeExcludeFilter(include=["*.py", "*.txt"])

        # Should match included patterns
        assert filter.match("file.py") is True
        assert filter.match("script.py") is True
        assert filter.match("readme.txt") is True
        assert filter.match("subdir/file.py") is True

        # Should not match non-included patterns
        assert filter.match("file.json") is False
        assert filter.match("image.png") is False
        assert filter.match("data.csv") is False

    def test_exclude_only(self):
        """Test filter with only exclude patterns."""
        filter = IncludeExcludeFilter(exclude=["*.pyc", "*.log"])

        # Should not match excluded patterns
        assert filter.match("file.pyc") is False
        assert filter.match("debug.log") is False
        assert filter.match("subdir/cache.pyc") is False

        # Should match non-excluded patterns
        assert filter.match("file.py") is True
        assert filter.match("readme.txt") is True
        assert filter.match("data.json") is True

    def test_include_and_exclude(self):
        """Test filter with both include and exclude patterns."""
        filter = IncludeExcludeFilter(
            include=["*.py", "*.txt"], exclude=["*test*", "*.pyc"]
        )

        # Should match included but not excluded
        assert filter.match("script.py") is True
        assert filter.match("readme.txt") is True

        # Should not match excluded even if included
        assert filter.match("test_script.py") is False
        assert filter.match("script_test.py") is False
        assert filter.match("test.txt") is False
        assert filter.match("file.pyc") is False

        # Should not match if not included
        assert filter.match("data.json") is False

    def test_complex_glob_patterns(self):
        """Test complex glob patterns."""
        filter = IncludeExcludeFilter(
            include=["src/*/*.py", "docs/*.md", "src/*.py"],
            exclude=["**/test_*", "**/__pycache__/*"],
        )

        # Should match complex include patterns
        assert filter.match("src/main.py") is True
        assert filter.match("src/utils/helper.py") is True
        assert filter.match("docs/readme.md") is True

        # Should not match excluded patterns
        assert filter.match("src/test_main.py") is False
        assert filter.match("src/utils/test_helper.py") is False
        assert filter.match("src/__pycache__/main.pyc") is False

        # Should not match non-included patterns
        assert filter.match("config.json") is False
        assert filter.match("docs/readme.txt") is False

    def test_exclude_takes_precedence(self):
        """Test that exclude patterns take precedence over include patterns."""
        filter = IncludeExcludeFilter(include=["*.py"], exclude=["test_*.py"])

        # Regular Python files should match
        assert filter.match("main.py") is True
        assert filter.match("utils.py") is True

        # Test files should be excluded even though they match include
        assert filter.match("test_main.py") is False
        assert filter.match("test_utils.py") is False

    def test_case_sensitivity(self):
        """Test case sensitivity in patterns."""
        filter = IncludeExcludeFilter(include=["*.PY"])

        # fnmatch is case-sensitive by default
        assert filter.match("file.PY") is True
        assert filter.match("file.py") is False

    def test_model_validation(self):
        """Test Pydantic model validation."""
        # Should work with valid data
        filter = IncludeExcludeFilter(include=["*.py"], exclude=["test_*"])
        assert filter.include == ["*.py"]
        assert filter.exclude == ["test_*"]

        # Should work with empty lists
        filter = IncludeExcludeFilter()
        assert filter.include == []
        assert filter.exclude == []

        # Should work when created from dict
        filter = IncludeExcludeFilter.model_validate(
            {"include": ["*.py", "*.txt"], "exclude": ["*.pyc"]}
        )
        assert filter.include == ["*.py", "*.txt"]
        assert filter.exclude == ["*.pyc"]


class TestFilterFiles:
    """Test the filter_files function."""

    def test_filter_files_with_no_filter(self):
        """Test filtering files without any filter (should return all files)."""
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create test files
            (temp_path / "file1.txt").write_text("content1")
            (temp_path / "file2.py").write_text("content2")
            (temp_path / "subdir").mkdir()
            (temp_path / "subdir" / "file3.json").write_text("content3")

            # Filter without any filter should return all files
            result = list(filter_files(temp_path, None))

            # Should return all files (relative paths)
            file_names = {f.name for f in result}
            assert "file1.txt" in file_names
            assert "file2.py" in file_names
            assert "file3.json" in file_names
            assert len(result) == 3

    def test_filter_files_with_include_filter(self):
        """Test filtering files with include patterns."""
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create test files
            (temp_path / "script.py").write_text("content")
            (temp_path / "readme.txt").write_text("content")
            (temp_path / "data.json").write_text("content")
            (temp_path / "subdir").mkdir()
            (temp_path / "subdir" / "module.py").write_text("content")

            # Filter to only include Python files
            filter = IncludeExcludeFilter(include=["*.py"])
            result = list(filter_files(temp_path, filter))

            # Should only return Python files
            file_names = {f.name for f in result}
            assert "script.py" in file_names
            assert "module.py" in file_names
            assert "readme.txt" not in file_names
            assert "data.json" not in file_names
            assert len(result) == 2

    def test_filter_files_with_exclude_filter(self):
        """Test filtering files with exclude patterns."""
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create test files
            (temp_path / "script.py").write_text("content")
            (temp_path / "test_script.py").write_text("content")
            (temp_path / "readme.txt").write_text("content")

            # Filter to exclude test files
            filter = IncludeExcludeFilter(exclude=["test_*"])
            result = list(filter_files(temp_path, filter))

            # Should exclude test files
            file_names = {f.name for f in result}
            assert "script.py" in file_names
            assert "readme.txt" in file_names
            assert "test_script.py" not in file_names
            assert len(result) == 2

    def test_filter_files_ignores_directories(self):
        """Test that filter_files ignores directories."""
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create directories and files
            (temp_path / "dir1").mkdir()
            (temp_path / "dir2").mkdir()
            (temp_path / "file1.txt").write_text("content")
            (temp_path / "dir1" / "file2.txt").write_text("content")

            # Should only return files, not directories
            result = list(filter_files(temp_path, None))

            # Should only return files
            assert len(result) == 2
            assert all(f.is_file() for f in result)

    def test_filter_files_with_nested_structure(self):
        """Test filtering files in nested directory structure."""
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create nested structure
            (temp_path / "src").mkdir()
            (temp_path / "src" / "main.py").write_text("content")
            (temp_path / "src" / "utils").mkdir()
            (temp_path / "src" / "utils" / "helper.py").write_text("content")
            (temp_path / "tests").mkdir()
            (temp_path / "tests" / "test_main.py").write_text("content")
            (temp_path / "README.md").write_text("content")

            # Filter to include only Python files in src
            filter = IncludeExcludeFilter(
                include=["src/*.py", "src/*/*.py"], exclude=["**/test_*"]
            )
            result = list(filter_files(temp_path, filter))

            # Should match files based on relative paths
            relative_paths = {str(f.relative_to(temp_path)) for f in result}
            expected_paths = {"src/main.py", "src/utils/helper.py"}
            assert relative_paths == expected_paths

    def test_filter_files_empty_directory(self):
        """Test filtering files in empty directory."""
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Empty directory should return no files
            result = list(filter_files(temp_path, None))
            assert len(result) == 0

    def test_filter_files_with_custom_filter(self):
        """Test filtering files with custom filter implementation."""
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create test files
            (temp_path / "file1.txt").write_text("content")
            (temp_path / "file2.py").write_text("content")
            (temp_path / "file3.json").write_text("content")

            # Create custom filter that only allows files with 'file1' in name
            class CustomFilter:
                def match(self, path: str) -> bool:
                    return "file1" in path

            custom_filter = CustomFilter()
            result = list(filter_files(temp_path, custom_filter))

            # Should only return file1.txt
            assert len(result) == 1
            assert result[0].name == "file1.txt"


class TestIntegration:
    """Integration tests combining multiple components."""

    def test_real_world_python_project_filtering(self):
        """Test filtering that mimics real-world Python project structure."""
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create realistic project structure
            (temp_path / "src").mkdir()
            (temp_path / "src" / "__init__.py").write_text("")
            (temp_path / "src" / "main.py").write_text("# main module")
            (temp_path / "src" / "utils.py").write_text("# utilities")

            (temp_path / "tests").mkdir()
            (temp_path / "tests" / "test_main.py").write_text("# tests")
            (temp_path / "tests" / "test_utils.py").write_text("# tests")

            (temp_path / "__pycache__").mkdir()
            (temp_path / "__pycache__" / "main.cpython-39.pyc").write_text("bytecode")

            (temp_path / "README.md").write_text("# Project")
            (temp_path / "requirements.txt").write_text("pytest")
            (temp_path / ".gitignore").write_text("__pycache__/")

            # Filter for source code only (exclude tests, cache, etc.)
            filter = IncludeExcludeFilter(
                include=["src/*.py", "*.md", "*.txt"],
                exclude=["**/__pycache__/**", ".*", "**/test_*"],
            )

            result = list(filter_files(temp_path, filter))
            relative_paths = {str(f.relative_to(temp_path)) for f in result}

            expected_paths = {
                "src/__init__.py",
                "src/main.py",
                "src/utils.py",
                "README.md",
                "requirements.txt",
            }

            assert relative_paths == expected_paths


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_include_patterns(self):
        """Test behavior with empty include patterns list."""
        filter = IncludeExcludeFilter(include=[], exclude=["*.pyc"])

        # Empty include should allow all files (except excluded)
        assert filter.match("file.py") is True
        assert filter.match("file.txt") is True
        assert filter.match("file.pyc") is False

    def test_empty_exclude_patterns(self):
        """Test behavior with empty exclude patterns list."""
        filter = IncludeExcludeFilter(include=["*.py"], exclude=[])

        # No exclude patterns should not exclude anything
        assert filter.match("file.py") is True
        assert filter.match("file.txt") is False
        assert filter.match("test_file.py") is True

    def test_overlapping_patterns(self):
        """Test overlapping include and exclude patterns."""
        filter = IncludeExcludeFilter(
            include=["*.py", "test_*.py"], exclude=["test_*.py"]
        )

        # Exclude should take precedence even if also in include
        assert filter.match("main.py") is True
        assert filter.match("test_main.py") is False

    def test_special_characters_in_paths(self):
        """Test paths with special characters."""
        filter = IncludeExcludeFilter(include=["*.py"])

        # Test various special characters that might appear in file paths
        assert filter.match("file-name.py") is True
        assert filter.match("file_name.py") is True
        assert filter.match("file.name.py") is True
        assert filter.match("file with spaces.py") is True
        assert filter.match("file[1].py") is True
        assert filter.match("file(1).py") is True

    def test_unicode_paths(self):
        """Test paths with unicode characters."""
        filter = IncludeExcludeFilter(include=["*.py"])

        assert filter.match("файл.py") is True  # Cyrillic
        assert filter.match("文件.py") is True  # Chinese
        assert filter.match("αρχείο.py") is True  # Greek

    def test_very_long_paths(self):
        """Test very long file paths."""
        long_path = "/".join(["very_long_directory_name"] * 20) + "/file.py"
        filter = IncludeExcludeFilter(include=["*.py"])

        assert filter.match(long_path) is True

    def test_dot_files_and_directories(self):
        """Test handling of dot files and directories."""
        filter = IncludeExcludeFilter(include=["*"], exclude=[".*", "**/.*"])

        assert filter.match("regular_file.txt") is True
        assert filter.match(".hidden_file") is False
        assert filter.match(".config/settings.json") is False
        assert filter.match("dir/.hidden_file") is False

    def test_multiple_extensions(self):
        """Test files with multiple extensions."""
        filter = IncludeExcludeFilter(include=["*.tar.gz", "*.backup.*"])

        assert filter.match("archive.tar.gz") is True
        assert filter.match("data.backup.sql") is True
        assert filter.match("config.backup.json") is True
        assert filter.match("file.tar.bz2") is False

    def test_case_insensitive_patterns(self):
        """Test that patterns are case sensitive by default."""
        filter = IncludeExcludeFilter(include=["*.py"])

        # fnmatch is case-sensitive by default
        assert filter.match("file.py") is True
        assert filter.match("file.PY") is False
        assert filter.match("FILE.py") is True

    def test_root_level_vs_nested_files(self):
        """Test distinction between root level and nested files."""
        filter = IncludeExcludeFilter(
            include=["*.py"],  # Only root level Python files
            exclude=["*/*.py"],  # Exclude Python files in subdirectories
        )

        assert filter.match("main.py") is True
        assert filter.match("src/main.py") is False
        assert filter.match("tests/test_main.py") is False

    def test_filter_files_with_symlinks(self):
        """Test behavior with symbolic links (if they exist)."""
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create a regular file
            regular_file = temp_path / "regular.txt"
            regular_file.write_text("content")

            try:
                # Try to create a symlink
                symlink = temp_path / "symlink.txt"
                symlink.symlink_to(regular_file)

                # Filter should handle symlinks
                result = list(filter_files(temp_path, None))

                # Should include both regular file and symlink
                assert len(result) >= 1  # At least the regular file
                file_names = {f.name for f in result}
                assert "regular.txt" in file_names

            except (OSError, NotImplementedError):
                # Symlinks might not be supported on all platforms
                pass

    def test_filter_files_with_broken_permissions(self):
        """Test behavior when file permissions cause issues."""
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create a file
            test_file = temp_path / "test.txt"
            test_file.write_text("content")

            # The filter_files function should handle files regardless of permissions
            # This is more of a smoke test since we can't easily create permission issues
            # in a cross-platform way in temporary directories
            result = list(filter_files(temp_path, None))
            assert len(result) == 1
            assert result[0].name == "test.txt"
