import json
import logging
from typing import Dict, Any, Callable, List

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False

from .core import Pipeline, ExecutionContext
from .registry import Operation

log = logging.getLogger(__name__)

class PipelineParser:
    """
    Parses YAML or JSON configs into a fully executable Pipeline.
    """
    
    @staticmethod
    def _resolve_function(operation_path: str) -> Callable:
        """Looks up a function inside the Operation registry."""
        for type_id, op_class in Operation._OPERATION_REGISTRY.items():
            if operation_path in getattr(op_class, "_function_registry", {}):
                return op_class.get_operation(operation_path)
        
        raise ValueError(f"Operation '{operation_path}' not found in any registry.")
    
    @staticmethod
    def build_step_runner(steps_config: List[Dict]) -> Callable:
        """
        Composes multiple operations into a single sequential Callable.
        The output of Step N is automatically passed as the FIRST positional argument to Step N+1.
        """
        resolved_steps = []
        for step in steps_config:
            op_path = step["operation"]
            func = PipelineParser._resolve_function(op_path)
            resolved_steps.append({
                "func": func,
                "name": op_path,
                "kwargs": step.get("kwargs", {})
            })
        
        def step_runner(*args, **kwargs):
            current_data = None
            for i, step in enumerate(resolved_steps):
                func = step["func"]
                step_kwargs = step["kwargs"]

                log.debug(f" -> Executing inner step: {step['name']}")

                if i == 0:
                    # Step 1: Receives the initial DAG inputs (e.g., from depends_on)
                    merged_kwargs = {**kwargs, **step_kwargs}
                    current_data = func(*args, **merged_kwargs)
                else:
                    # Step N: Receives the output of Step N-1 as the first argument
                    current_data = func(current_data, **step_kwargs)
            
            return current_data
        
        step_runner.__name__ = "chained_steps"
        return step_runner
    
    @staticmethod
    def from_dict(config: Dict[str, Any]) -> Pipeline:
        pipeline_id = config.get("pipeline", "default_pipeline")
        
        # 1. Build the Execution Context from the Resources block
        context = ExecutionContext()
        for res_cfg in config.get("resources", []):
            adapter_name = res_cfg["adapter"]
            # Assuming resources are registered under "resource.{adapter_name}"
            try:
                # AdapterClass = Operation.get_operation_type(f"resource").get_operation(adapter_name)
                print(Operation._OPERATION_REGISTRY)
                AdapterClass = PipelineParser._resolve_function(adapter_name)
            except KeyError:
                # Fallback if registered directly without sub-types
                AdapterClass = Operation.get_operation(adapter_name)
                
            adapter_instance = AdapterClass(**res_cfg.get("params", {}))
            context.register_resource(res_cfg["id"], adapter_instance)

        # 2. Initialize the Pipeline with the prepared context
        pipe = Pipeline(id=pipeline_id, context=context)

        # 3. Add the tasks
        for task_cfg in config.get("tasks", []):
            task_id = task_cfg["id"]
            if "steps" in task_cfg:
                func = PipelineParser.build_step_runner(task_cfg["steps"])
            else:
                func = PipelineParser._resolve_function(task_cfg["operation"])

            # Map depends_on strings to actual TaskNode references
            kwargs = task_cfg.get("kwargs", {})
            for kwarg_name, parent_id in task_cfg.get("depends_on", {}).items():
                kwargs[kwarg_name] = pipe.nodes[parent_id]

            # Use the new API
            from .core import TaskNode
            pipe.add_node(TaskNode(
                id=task_id,
                func=func,
                args=tuple(task_cfg.get("args", [])),
                kwargs=kwargs,
                retries=task_cfg.get("retries", 3),
                retry_delay=task_cfg.get("retry_delay", 1.0)
            ))

        return pipe


    @staticmethod
    def from_json(filepath: str) -> Pipeline:
        with open(filepath, "r") as f:
            config = json.load(f)
        return PipelineParser.from_dict(config)
    
    @staticmethod
    def from_yaml(filepath: str) -> Pipeline:
        if not HAS_YAML:
            raise ImportError("PyYAML is not installed.")
        with open(filepath, "r") as f:
            config = yaml.safe_load(f)
        return PipelineParser.from_dict(config)