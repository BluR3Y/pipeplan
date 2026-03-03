import logging
import tempfile
import json
from pipeplan import Pipeline, Task, ExecutionContext
from pipeplan.resources import FileResource
from pipeplan.transfers import TransferOps
from pipeplan.transforms import ElementTransform, SetTransform

# Setup Logging so we see output instead of print() statements
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

def main():
    # 1. Setup Mock Source Data
    source_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix=".json")
    mock_data = [
        {"id": 1, "name": "  apple_bad  ", "score": 10},
        {"id": 2, "name": "banana", "score": 2},
        {"id": 3, "name": "cherry_bad", "score": 15},
    ]
    json.dump(mock_data, source_file)
    source_file.close()

    # 2. Setup Context & Resources dynamically mapped to THIS run
    context = ExecutionContext()
    context.register_resource("source_json", FileResource.get_operation("json")(path=source_file.name, mode="r"))

    # 3. Create Pipeline attached to the context
    pipeline = Pipeline("Daily_ETL", context=context)

    # 4. Construct Tasks (Retrieving Operations from the Registry)
    extract_fn = TransferOps.get_operation("extract")
    clean_fn = ElementTransform.get_operation("clean_strings")
    filter_fn = SetTransform.get_operation("filter_rows")

    task_extract = Task(
        id="extract_data",
        action=extract_fn,
        params={"resource_id": "source_json", "as_dataframe": True}
    )

    # Note the kwargs mapping: "df" input maps to the output of "extract_data"
    task_clean = Task(
        id="clean_names",
        action=clean_fn,
        depends_on={"df": "extract_data"}, 
        params={"column": "name", "replace": "_bad"}
    )

    task_filter = Task(
        id="filter_scores",
        action=filter_fn,
        depends_on={"df": "clean_names"},
        params={"column": "score", "threshold": 5}
    )
    
    # 5. Build DAG & Run
    pipeline.add_task(task_extract)
    pipeline.add_task(task_clean)
    pipeline.add_task(task_filter)

    pipeline.run()

    # 6. View Results natively
    final_output = pipeline.tasks["filter_scores"].output
    logging.info(f"Final Data Output:\n{final_output}")

if __name__ == "__main__":
    main()