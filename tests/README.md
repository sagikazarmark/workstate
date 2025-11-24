# Workstate Test Suite

This directory contains comprehensive tests for the workstate package, which provides protocols and implementations for managing state between durable function steps.

## Test Structure

```
tests/
├── README.md                    # This file
├── conftest.py                  # Test configuration and fixtures
├── test_workstate.py           # Main module tests
├── test_file.py                # Protocol tests
├── test_error_handling.py      # Error handling and edge cases
├── test_performance.py         # Performance and benchmark tests
├── test_integration.py         # Integration tests
└── obstore/                    # obstore-specific tests
    ├── __init__.py
    ├── test_obstore.py         # obstore module tests
    └── test_file.py            # obstore implementation tests
```

## Test Categories

### Unit Tests
- **Protocol Tests** (`test_file.py`): Test the FileLoader and FilePersister protocols
- **Module Tests** (`test_workstate.py`): Test main module imports and exports
- **Implementation Tests** (`obstore/test_file.py`): Test obstore-specific implementations

### Integration Tests
- **Obstore Integration** (`test_integration.py`): Full roundtrip tests with mocked obstore
- **Protocol Compliance**: Verify implementations conform to protocols

### Performance Tests
- **Scalability** (`test_performance.py`): Test performance with various data sizes
- **Memory Efficiency**: Test memory usage patterns
- **Bulk Operations**: Test handling of multiple operations

### Error Handling Tests
- **Edge Cases** (`test_error_handling.py`): URL parsing, invalid inputs, file system errors
- **Resource Management**: Test proper cleanup and error recovery

## Running Tests

### Basic Test Run
```bash
python -m pytest tests/ -v
```

### Fast Tests Only (exclude slow/integration)
```bash
python -m pytest tests/ -m "not slow and not integration" -v
```

### Integration Tests Only
```bash
python -m pytest tests/ -m integration -v
```

### Performance Tests
```bash
python -m pytest tests/ -m slow -v
```

### With Coverage
```bash
python -m pytest tests/ --cov=workstate --cov-report=term-missing --cov-report=html
```

### Using the Test Runner
```bash
# Basic run
python run_tests.py

# With coverage
python run_tests.py --coverage

# Fast tests only
python run_tests.py --fast

# Integration tests
python run_tests.py --integration

# Specific test file
python run_tests.py --file tests/test_file.py

# Specific test
python run_tests.py --test test_load_returns_io
```

## Test Markers

The test suite uses pytest markers to categorize tests:

- `@pytest.mark.integration`: Integration tests that require obstore
- `@pytest.mark.slow`: Performance and benchmark tests that take longer to run

## Test Fixtures

Common fixtures are defined in `conftest.py`:

- `sample_bytes_data`: Sample bytes data for testing
- `sample_bytearray_data`: Sample bytearray data
- `sample_memoryview_data`: Sample memoryview data
- `sample_urls`: Dictionary of various URL types for testing
- `mock_client_options`: Mock client configuration options

## Test Dependencies

### Required Dependencies
- `pytest`: Test framework
- `pydantic`: For URL validation (part of main dependencies)

### Optional Dependencies
- `obstore`: Required for obstore-specific tests and integration tests
- `pytest-cov`: For coverage reporting

### Development Dependencies
```bash
pip install pytest pytest-cov
pip install workstate[obstore]  # For obstore tests
```

## Writing New Tests

### Test Structure Guidelines

1. **Test Classes**: Group related tests in classes using descriptive names
   ```python
   class TestFileLoaderProtocol:
       """Test the FileLoader protocol."""
       
       def test_protocol_methods_exist(self):
           """Test that FileLoader has expected methods."""
           # Test implementation
   ```

2. **Test Methods**: Use descriptive names that explain what is being tested
   ```python
   def test_load_with_path_destination(self):
       """Test load method with Path destination."""
       # Test implementation
   ```

3. **Docstrings**: Include docstrings explaining the test purpose

4. **Assertions**: Use descriptive assertions with helpful error messages
   ```python
   assert result is not None, "Load operation should return a result"
   ```

### Mocking Guidelines

1. **Mock External Dependencies**: Always mock obstore operations
   ```python
   @patch("workstate.obstore.file.obstore.get")
   def test_load_operation(self, mock_get):
       # Setup mock
       mock_result = Mock()
       mock_get.return_value = mock_result
       # Test implementation
   ```

2. **Mock at the Right Level**: Mock at the boundary of your code
3. **Verify Interactions**: Check that mocks were called correctly
   ```python
   mock_get.assert_called_once_with(mock_store, "expected/path")
   ```

### Test Data Guidelines

1. **Use Realistic Data**: Create test data that represents real-world scenarios
2. **Test Edge Cases**: Include empty data, large data, special characters
3. **Use Fixtures**: Share common test data through fixtures

## Coverage Goals

The test suite aims for high coverage across all modules:

- **Line Coverage**: >90% of executable lines
- **Branch Coverage**: >85% of conditional branches
- **Function Coverage**: 100% of public functions

### Current Coverage Areas

- ✅ Protocol definitions and imports
- ✅ obstore FileLoader implementation
- ✅ obstore FilePersister implementation
- ✅ URL parsing and store resolution
- ✅ Error handling and edge cases
- ✅ Different data types (bytes, bytearray, memoryview, Path)
- ✅ IO operations and file handling
- ✅ Performance characteristics

## Continuous Integration

Tests are designed to run in CI environments:

- **Fast Tests**: Run on every commit (exclude slow/integration markers)
- **Full Test Suite**: Run on pull requests and releases
- **Performance Tests**: Run periodically to detect regressions

## Troubleshooting

### Common Issues

1. **Missing obstore dependency**:
   ```
   pytest.importorskip("obstore")  # Will skip tests if not available
   ```

2. **Mock assertions failing**:
   - Check that you're mocking the right path
   - Verify expected call arguments match actual calls
   - Use `mock.call_args_list` to debug call history

3. **Performance tests failing**:
   - Performance thresholds may need adjustment for different environments
   - Use `@pytest.mark.slow` to exclude from fast test runs

### Debugging Tests

1. **Verbose Output**: Use `-v` flag for detailed test output
2. **Print Debugging**: Add print statements (remove before commit)
3. **PDB Debugging**: Use `--pdb` flag to drop into debugger on failures
4. **Mock Inspection**: Print `mock.call_args_list` to see all calls

## Contributing

When contributing new tests:

1. **Follow Naming Conventions**: Use descriptive test and class names
2. **Add Appropriate Markers**: Mark slow or integration tests
3. **Update Documentation**: Update this README if adding new test categories
4. **Maintain Coverage**: Ensure new code is covered by tests
5. **Test Edge Cases**: Consider error conditions and boundary cases

## Test Philosophy

The workstate test suite follows these principles:

1. **Fast by Default**: Most tests should run quickly
2. **Comprehensive Coverage**: Test all public APIs and edge cases
3. **Clear Intent**: Test names and structure should be self-documenting
4. **Reliable**: Tests should not be flaky or environment-dependent
5. **Maintainable**: Tests should be easy to update when code changes

For questions about testing or to report issues with the test suite, please open an issue in the project repository.