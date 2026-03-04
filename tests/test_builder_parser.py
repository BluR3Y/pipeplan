import pytest
from pipeplan.builder import Builder
from pipeplan.parser import PipelineParser

def test_builder_generates_correct_dict():
    """Validates that the Builder creates the expected JSON/YAML-compatible dictionary."""
    builder = (
        Builder()
        .resource(id="db", adapter="postgres", params={"host": "localhost"})
        .task(id="t1", op="extract")
        .task(id="t2", steps=[{"operation": "clean"}], depends_on={"df": "t1"})
    )
    
    cfg = builder.to_dict()
    
    assert len(cfg["resources"]) == 1
    assert cfg["resources"][0]["adapter"] == "postgres"
    assert len(cfg["tasks"]) == 2
    assert cfg["tasks"][0]["operation"] == "extract"
    assert "steps" in cfg["tasks"][1]
    assert cfg["tasks"][1]["depends_on"]["df"] == "t1"

def test_parser_build_step_runner():
    """Validates that grouped linear steps pass data sequentially."""
    
    # We must register dummy operations to the registry to test the parser
    from pipeplan.registry import Operation
    
    class DummyOps(Operation, type_id="dummy"): pass
    
    @DummyOps.register_operation("add_one")
    def add_one(x): return x + 1
    
    @DummyOps.register_operation("multiply_two")
    def multiply_two(x): return x * 2

    # Configure a step chain: (x + 1) * 2
    steps_config = [
        {"operation": "add_one"},
        {"operation": "multiply_two"}
    ]
    
    # Build the composed callable
    step_runner = PipelineParser.build_step_runner(steps_config)
    
    # Execute the chained function directly: (3 + 1) * 2 = 8
    result = step_runner(x=3)
    assert result == 8