#!/usr/bin/env python
"""
Debug script for running tests with VS Code debugger
"""
import os
import sys
import pytest

# Set up the environment for the test
os.environ['PYTHONPATH'] = os.path.dirname(os.path.abspath(__file__))

# Run the test with pytest
if __name__ == "__main__":
    # You can modify this to run specific tests
    test_path = "tests/integration/caching_test/test_caching.py::TestBaseCaching"
    sys.exit(pytest.main(["-v", test_path]))
