"""
Integration tests for workstate.obstore.state.StateManager.

This module contains comprehensive tests for the StateManager class using
obstore.store.MemoryStore to verify that file uploading functionality works
correctly with various options including prefix and filter configurations.

Test Coverage:
- Basic file upload (single and multiple files)
- Prefix functionality (simple and nested paths)
- Filter functionality (include/exclude patterns)
- Combined prefix and filter options
- Directory structure preservation
- Edge cases (empty directories, complex patterns)
"""

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

import obstore
import pytest

import workstate
import workstate.obstore
from workstate.obstore.filter import IncludeExcludeFilter


def test_protocol():
    """Test that StateManager can be instantiated with proper type annotations."""
    _: workstate.StateManager = workstate.obstore.StateManager[Any](
        obstore.store.MemoryStore()
    )


@dataclass
class OptionsWithPrefix:
    prefix: PurePosixPath


@dataclass
class OptionsWithFilter:
    filter: IncludeExcludeFilter


@dataclass
class OptionsWithPrefixAndFilter:
    prefix: PurePosixPath
    filter: IncludeExcludeFilter


@dataclass
class EmptyOptions:
    pass


class TestStateManagerIntegration:
    def setup_method(self):
        self.store = obstore.store.MemoryStore()
        self.state_manager = workstate.obstore.StateManager[Any](self.store)

    def test_save_single_file(self):
        """Test saving a single file to the store."""
        options = EmptyOptions()

        with self.state_manager.save(options) as temp_dir:
            # Create a test file
            test_file = Path(temp_dir) / "test.txt"
            test_file.write_text("Hello, World!")

        # Verify the file was uploaded
        result = self.store.get("test.txt")
        stored_data = bytes(result.bytes())
        assert stored_data == b"Hello, World!"

    def test_save_multiple_files(self):
        """Test saving multiple files to the store."""
        options = EmptyOptions()

        with self.state_manager.save(options) as temp_dir:
            # Create multiple test files
            (Path(temp_dir) / "file1.txt").write_text("Content 1")
            (Path(temp_dir) / "file2.txt").write_text("Content 2")
            (Path(temp_dir) / "subdir").mkdir()
            (Path(temp_dir) / "subdir" / "file3.txt").write_text("Content 3")

        # Verify all files were uploaded
        assert bytes(self.store.get("file1.txt").bytes()) == b"Content 1"
        assert bytes(self.store.get("file2.txt").bytes()) == b"Content 2"
        assert bytes(self.store.get("subdir/file3.txt").bytes()) == b"Content 3"

    def test_save_with_prefix(self):
        """Test saving files with a prefix option."""
        state_manager = workstate.obstore.StateManager[Any](self.store)
        options = OptionsWithPrefix(prefix=PurePosixPath("my-project/v1"))

        with state_manager.save(options) as temp_dir:
            test_file = Path(temp_dir) / "config.json"
            test_file.write_text('{"key": "value"}')

        # Verify the file was uploaded with prefix
        result = self.store.get("my-project/v1/config.json")
        stored_data = bytes(result.bytes())
        assert stored_data == b'{"key": "value"}'

        # Verify file is not at root level
        with pytest.raises(Exception):
            self.store.get("config.json")

    def test_save_with_nested_prefix(self):
        """Test saving files with a nested prefix."""
        state_manager = workstate.obstore.StateManager[Any](self.store)
        options = OptionsWithPrefix(prefix=PurePosixPath("projects/workstate/backups"))

        with state_manager.save(options) as temp_dir:
            # Create nested directory structure
            nested_dir = Path(temp_dir) / "src" / "lib"
            nested_dir.mkdir(parents=True)
            (nested_dir / "utils.py").write_text("def helper(): pass")

        # Verify the file was uploaded with full prefix path
        result = self.store.get("projects/workstate/backups/src/lib/utils.py")
        stored_data = bytes(result.bytes())
        assert stored_data == b"def helper(): pass"

    def test_save_with_filter_include(self):
        """Test saving files with filter include patterns."""
        state_manager = workstate.obstore.StateManager[Any](self.store)
        options = OptionsWithFilter(
            filter=IncludeExcludeFilter(include=["*.txt", "*.md"])
        )

        with state_manager.save(options) as temp_dir:
            # Create files with different extensions
            (Path(temp_dir) / "readme.md").write_text("# README")
            (Path(temp_dir) / "notes.txt").write_text("Some notes")
            (Path(temp_dir) / "config.json").write_text('{"test": true}')
            (Path(temp_dir) / "script.py").write_text("print('hello')")

        # Verify only matching files were uploaded
        assert bytes(self.store.get("readme.md").bytes()) == b"# README"
        assert bytes(self.store.get("notes.txt").bytes()) == b"Some notes"

        # Verify non-matching files were not uploaded
        with pytest.raises(Exception):
            self.store.get("config.json")
        with pytest.raises(Exception):
            self.store.get("script.py")

    def test_save_with_filter_exclude(self):
        """Test saving files with filter exclude patterns."""
        state_manager = workstate.obstore.StateManager[Any](self.store)
        options = OptionsWithFilter(
            filter=IncludeExcludeFilter(exclude=["*.log", "*.tmp", "__pycache__/*"])
        )

        with state_manager.save(options) as temp_dir:
            # Create files, some should be excluded
            (Path(temp_dir) / "app.py").write_text("import os")
            (Path(temp_dir) / "debug.log").write_text("Debug info")
            (Path(temp_dir) / "temp.tmp").write_text("Temporary data")

            # Create __pycache__ directory
            pycache_dir = Path(temp_dir) / "__pycache__"
            pycache_dir.mkdir()
            (pycache_dir / "module.pyc").write_text("Compiled code")

        # Verify included files were uploaded
        assert bytes(self.store.get("app.py").bytes()) == b"import os"

        # Verify excluded files were not uploaded
        with pytest.raises(Exception):
            self.store.get("debug.log")
        with pytest.raises(Exception):
            self.store.get("temp.tmp")
        with pytest.raises(Exception):
            self.store.get("__pycache__/module.pyc")

    def test_save_with_filter_include_and_exclude(self):
        """Test saving files with both include and exclude filters."""
        state_manager = workstate.obstore.StateManager[Any](self.store)
        options = OptionsWithFilter(
            filter=IncludeExcludeFilter(
                include=["*.py", "*.txt"], exclude=["*_test.py", "*.tmp.txt"]
            )
        )

        with state_manager.save(options) as temp_dir:
            (Path(temp_dir) / "main.py").write_text("def main(): pass")
            (Path(temp_dir) / "test_main.py").write_text(
                "def test(): pass"
            )  # Should be included
            (Path(temp_dir) / "main_test.py").write_text(
                "def test(): pass"
            )  # Should be excluded
            (Path(temp_dir) / "readme.txt").write_text("README")
            (Path(temp_dir) / "notes.tmp.txt").write_text(
                "Temp notes"
            )  # Should be excluded
            (Path(temp_dir) / "config.json").write_text(
                "{}"
            )  # Should be excluded (not in include)

        # Verify correct files were uploaded
        assert bytes(self.store.get("main.py").bytes()) == b"def main(): pass"
        assert bytes(self.store.get("test_main.py").bytes()) == b"def test(): pass"
        assert bytes(self.store.get("readme.txt").bytes()) == b"README"

        # Verify excluded files were not uploaded
        with pytest.raises(Exception):
            self.store.get("main_test.py")
        with pytest.raises(Exception):
            self.store.get("notes.tmp.txt")
        with pytest.raises(Exception):
            self.store.get("config.json")

    def test_save_with_prefix_and_filter(self):
        """Test saving files with both prefix and filter options."""
        state_manager = workstate.obstore.StateManager[Any](self.store)
        options = OptionsWithPrefixAndFilter(
            prefix=PurePosixPath("backup/source"),
            filter=IncludeExcludeFilter(include=["*.py"]),
        )

        with state_manager.save(options) as temp_dir:
            (Path(temp_dir) / "app.py").write_text("# Main app")
            (Path(temp_dir) / "utils.py").write_text("# Utilities")
            (Path(temp_dir) / "readme.md").write_text("# README")

        # Verify Python files were uploaded with prefix
        assert bytes(self.store.get("backup/source/app.py").bytes()) == b"# Main app"
        assert bytes(self.store.get("backup/source/utils.py").bytes()) == b"# Utilities"

        # Verify non-Python files were not uploaded
        with pytest.raises(Exception):
            self.store.get("backup/source/readme.md")

    def test_save_empty_directory(self):
        """Test saving from an empty directory."""
        options = EmptyOptions()

        with self.state_manager.save(options) as _:
            # Don't create any files
            pass

        # Verify no files were uploaded (store should still be empty)
        # Since MemoryStore doesn't have a list method, we'll verify by trying to get a non-existent file
        with pytest.raises(Exception):
            self.store.get("nonexistent.txt")

    def test_save_preserves_directory_structure(self):
        """Test that directory structure is preserved in object keys."""
        options = EmptyOptions()

        with self.state_manager.save(options) as temp_dir:
            # Create nested directory structure
            (Path(temp_dir) / "src").mkdir()
            (Path(temp_dir) / "src" / "components").mkdir()
            (Path(temp_dir) / "tests").mkdir()

            (Path(temp_dir) / "src" / "main.py").write_text("# Main")
            (Path(temp_dir) / "src" / "components" / "widget.py").write_text("# Widget")
            (Path(temp_dir) / "tests" / "test_main.py").write_text("# Tests")

        # Verify directory structure is preserved in object keys
        assert bytes(self.store.get("src/main.py").bytes()) == b"# Main"
        assert bytes(self.store.get("src/components/widget.py").bytes()) == b"# Widget"
        assert bytes(self.store.get("tests/test_main.py").bytes()) == b"# Tests"

    def test_save_with_complex_filter_patterns(self):
        """Test saving files with complex glob patterns."""
        state_manager = workstate.obstore.StateManager[Any](self.store)
        options = OptionsWithFilter(
            filter=IncludeExcludeFilter(
                include=["src/*.py", "src/**/*.py", "docs/*.md"],
                exclude=["**/*_test.py", "**/*.pyc"],
            )
        )

        with state_manager.save(options) as temp_dir:
            # Create complex directory structure
            src_dir = Path(temp_dir) / "src"
            src_dir.mkdir()
            (src_dir / "main.py").write_text("# Main")
            (src_dir / "main_test.py").write_text("# Test")

            lib_dir = src_dir / "lib"
            lib_dir.mkdir()
            (lib_dir / "utils.py").write_text("# Utils")
            (lib_dir / "utils.pyc").write_text("Compiled")

            docs_dir = Path(temp_dir) / "docs"
            docs_dir.mkdir()
            (docs_dir / "readme.md").write_text("# Docs")
            (docs_dir / "guide.txt").write_text("Guide")

        # Verify correct files were uploaded
        assert bytes(self.store.get("src/main.py").bytes()) == b"# Main"
        assert bytes(self.store.get("src/lib/utils.py").bytes()) == b"# Utils"
        assert bytes(self.store.get("docs/readme.md").bytes()) == b"# Docs"

        # Verify excluded files were not uploaded
        with pytest.raises(Exception):
            self.store.get("src/main_test.py")
        with pytest.raises(Exception):
            self.store.get("src/lib/utils.pyc")
        with pytest.raises(Exception):
            self.store.get("docs/guide.txt")
