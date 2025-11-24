"""Test configuration and fixtures for workstate tests."""

import pytest


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "slow: marks tests as slow running tests")


@pytest.fixture
def sample_bytes_data():
    """Fixture providing sample bytes data for testing."""
    return b"sample test data for workstate tests"


@pytest.fixture
def sample_bytearray_data():
    """Fixture providing sample bytearray data for testing."""
    return bytearray(b"sample bytearray data")


@pytest.fixture
def sample_memoryview_data():
    """Fixture providing sample memoryview data for testing."""
    return memoryview(b"sample memoryview data")


@pytest.fixture
def sample_urls():
    """Fixture providing various sample URLs for testing."""
    return {
        "s3": "s3://test-bucket/test-key",
        "gs": "gs://test-bucket/test-key",
        "azure": "azure://test-container/test-key",
        "file": "file:///tmp/test-file",
        "http": "http://example.com/test-file",
        "https": "https://example.com/test-file",
    }


@pytest.fixture
def mock_client_options():
    """Fixture providing mock client options for testing."""
    return {
        "aws_access_key_id": "test_access_key",
        "aws_secret_access_key": "test_secret_key",
        "region": "us-west-2",
        "endpoint_url": "https://s3.amazonaws.com",
    }


def pytest_collection_modifyitems(config, items):
    """Modify test collection to handle special markers."""
    # Add slow marker to integration tests
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(pytest.mark.slow)
