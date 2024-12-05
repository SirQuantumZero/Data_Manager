# tests/conftest.py
import pytest
import logging
from pathlib import Path

def pytest_configure(config):
    """Add benchmark marker"""
    config.addinivalue_line(
        "markers", "benchmark: mark test as a performance benchmark"
    )

@pytest.fixture(scope="session")
def benchmark_logger():
    """Logger for performance benchmarks"""
    logger = logging.getLogger("benchmark")
    handler = logging.FileHandler('benchmark.log')
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger