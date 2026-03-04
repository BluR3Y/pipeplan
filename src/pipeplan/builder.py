from typing import Any, Dict, List, Optional

class Builder:
    """
    A fluent configuration builder that constructs a declarative
    blueprint of resources and tasks for the pipeline.
    """
    def __init__(self):
        self._config: Dict[str, Any] = {
            "resources": [],
            "tasks": []
        }
    
    def resource(self, id: str, adapter: str, params: Optional[Dict[str, Any]] = None) -> "Builder":
        self._config["resources"].append({
            "id": id,
            "adapter": adapter,
            "params": params or {}
        })
        return self
    
    def task(
        self,
        id: str,
        op: Optional[str] = None,
        steps: Optional[List[Dict[str, Any]]] = None,
        params: Optional[Dict[str, Any]] = None,
        depends_on: Optional[Dict[str, str]] = None
    ) -> "Builder":
        task_def = {"id": id}
        if op: task_def["operation"] = op
        elif steps: task_def["steps"] = steps
        else: raise ValueError(f"Task '{id}' must define either an 'op' or 'steps'.")

        if params: task_def["kwargs"] = params
        if depends_on: task_def["depends_on"] = depends_on

        self._config["tasks"].append(task_def)
        return self
    
    def to_dict(self) -> Dict[str, Any]:
        return self._config
    
    def build(self, pipeline_id: str) -> "Pipeline":
        """
        Inversion of Control: The Builder compiles itself into a Pipeline.
        This prevents the Pipeline engine from needing to know about parsing logic.
        """
        from .parser import PipelineParser
        cfg = self.to_dict()
        cfg["pipeline"] = pipeline_id
        return PipelineParser.from_dict(cfg)