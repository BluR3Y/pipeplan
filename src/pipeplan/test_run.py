import json
import logging
import tempfile
import pandas as pd

# pipeplan imports
from pipeplan.core import Pipeline, ExecutionContext, task
from pipeplan.builder import Builder
from pipeplan.resources import JsonAdapter
from pipeplan.transfers import _extract
from pipeplan.transforms import clean_strings

# Setup basic logging to see the pipeline execution steps
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

log = logging.getLogger(__name__)

def setup_mock_data() -> str:
    """Helper to create a temporary JSON file with messy data."""
    tmp = tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix=".json")
    json.dump([
        {"id": 1, "name": "  apple_bad  ", "score": 10},
        {"id": 2, "name": "banana", "score": 2},
        {"id": 3, "name": "cherry_bad", "score": 15},
    ], tmp)
    tmp.close()
    return tmp.name


# =====================================================================
# EXAMPLE 1: The Config Builder (Best for YAML/JSON & Reusability)
# =====================================================================
def run_builder_example(data_path: str):
    print("\n--- Running Example 1: The Builder API ---")
    
    # 1. Define the Blueprint (This could easily be loaded from a YAML file!)
    blueprint = (
        Builder()
        # Auto-managed resource connections
        .resource(id="my_json", adapter="json", params={"path": data_path})
        
        # Parallel extraction tasks
        .task(id="extract_raw", op="extract", params={"resource_id": "my_json", "as_dataframe": True})
        
        # Composed "Steps" to transform data sequentially
        .task(id="transform_all", depends_on={"df": "extract_raw"}, steps=[
            {"operation": "clean_strings", "kwargs": {"column": "name", "replace": "_bad"}},
            # Assuming you add filter_rows to transforms.py:
            # {"operation": "filter_rows", "kwargs": {"column": "score", "threshold": 5}} 
        ])
    )

    # 2. Compile the blueprint into a Pipeline
    pipe = blueprint.build(pipeline_id="Builder_ETL")

    # 3. Execute! (Contexts and file closing are handled automatically)
    pipe.run()
    
    # 4. View results
    print("Final Output:\n", pipe.nodes["transform_all"].result.output)


# =====================================================================
# EXAMPLE 2: TaskFlow Decorators (Best for pure Python workflows)
# =====================================================================
def run_taskflow_example(data_path: str):
    print("\n--- Running Example 2: The TaskFlow API ---")

    # 1. You can write custom tasks natively using the @task decorator!
    @task(retries=2)
    def my_custom_print(df: pd.DataFrame):
        log.info(f"Custom task received {len(df)} rows!")
        return df.to_dict(orient="records")

    # 2. Setup your execution context manually
    context = ExecutionContext()
    context.register_resource("source_json", JsonAdapter(path=data_path))

    # 3. Build the DAG implicitly using Python variables
    with context:
        with Pipeline("TaskFlow_ETL") as pipe:
            
            # extract and clean_strings are already registered as Operations!
            raw_df = _extract(resource_id="source_json", as_dataframe=True)
            cleaned_df = clean_strings(df=raw_df, column="name", replace="_bad")
            
            # Call our custom task
            final_data = my_custom_print(cleaned_df)

        # 4. Run the implicitly built pipeline
        pipe.run()
        
    print("Final Output:\n", final_data.result.output)


# =====================================================================
# EXAMPLE 3: Direct Method Chaining (Best for dynamically generated tasks)
# =====================================================================
def run_chaining_example(data_path: str):
    print("\n--- Running Example 3: Direct Method Chaining ---")

    context = ExecutionContext()
    context.register_resource("dynamic_json", JsonAdapter(path=data_path))

    # Let's pretend we loop over a list of files and dynamically add tasks
    pipe = Pipeline("Dynamic_ETL", context=context)
    
    # Chain tasks directly onto the mutable pipeline object
    pipe.add_task(
        id="extract_1", 
        func=_extract, 
        kwargs={"resource_id": "dynamic_json", "as_dataframe": True}
    ).add_task(
        id="clean_1", 
        func=clean_strings, 
        kwargs={"column": "name", "replace": "_bad"}, 
        depends_on={"df": "extract_1"}  # String mapping to the parent ID
    )

    pipe.run()
    print("Final Output:\n", pipe.nodes["clean_1"].result.output)


if __name__ == "__main__":
    # Create the dummy file once
    mock_file_path = setup_mock_data()
    
    # Run the examples
    run_builder_example(mock_file_path)
    run_taskflow_example(mock_file_path)
    run_chaining_example(mock_file_path)