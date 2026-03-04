import pytest
import tempfile
import json
import os
from pipeplan.core import ExecutionContext

@pytest.fixture
def sample_data():
    """Provides a static sample dataset for tests."""
    return [
        {"id": 1, "name": "  apple_bad  ", "score": 10},
        {"id": 2, "name": "banana", "score": 2},
        {"id": 3, "name": "cherry_bad", "score": 15},
    ]

@pytest.fixture
def mock_json_file(sample_data):
    """Creates a temporary JSON file and cleans it up after the test."""
    tmp = tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix=".json")
    json.dump(sample_data, tmp)
    tmp.close()
    
    yield tmp.name  # Pause here and hand the file path to the test
    
    # Cleanup after test finishes
    os.remove(tmp.name)

@pytest.fixture
def fresh_context():
    """Provides a fresh, isolated ExecutionContext for every test."""
    return ExecutionContext()