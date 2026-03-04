import pytest
import pandas as pd
import json
import os
import tempfile
from pipeplan.core import ExecutionContext, Pipeline
from pipeplan.resources import JsonAdapter
from pipeplan.transfers import extract
from pipeplan.transforms import clean_strings

# ==========================================
# Fixtures
# ==========================================
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


# ==========================================
# Tests
# ==========================================
def test_json_adapter(mock_json_file, sample_data):
    """Validates the ContextManager protocol and reading capability of JsonAdapter."""
    adapter = JsonAdapter(path=mock_json_file)
    
    with adapter:
        data = adapter.read()
        
    assert len(data) == 3
    assert data[0]["name"].strip() == "apple_bad"
    assert adapter.client.closed  # Asserts disconnect() was called

def test_extract_to_dataframe(mock_json_file, fresh_context):
    """Tests the `extract` operation and its pandas normalization logic."""
    fresh_context.register_resource("source", JsonAdapter(path=mock_json_file))
    
    with fresh_context:
        # We explicitly call _extract directly as a function (outside pipeline)
        df = extract(resource_id="source", as_dataframe=True)
        
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert "name" in df.columns

def test_clean_strings():
    """Tests the pandas transformation operation directly."""
    # Create raw dataframe
    df = pd.DataFrame([
        {"id": 1, "name": "  apple_bad  "},
        {"id": 2, "name": "banana"}
    ])
    
    # Run transform
    cleaned_df = clean_strings(df, column="name", replace="_bad")
    
    # Assertions
    assert cleaned_df.iloc[0]["name"] == "apple"  # '  apple_bad  ' -> 'apple'
    assert cleaned_df.iloc[1]["name"] == "banana"
    
def test_full_pipeline_integration(mock_json_file, fresh_context):
    """End-to-End integration test combining Resources, Transfers, and Transforms."""
    fresh_context.register_resource("source", JsonAdapter(path=mock_json_file))
    
    with fresh_context:
        with Pipeline("E2E_Test") as pipe:
            raw = extract("source", as_dataframe=True)
            cleaned = clean_strings(raw, column="name", replace="_bad")
            
        pipe.run()
        
    final_df = cleaned.result.output
    
    assert isinstance(final_df, pd.DataFrame)
    assert final_df.iloc[0]["name"] == "apple"
    assert final_df.iloc[2]["name"] == "cherry"