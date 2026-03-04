from .resources import Resource
from typing import Any, Dict
import logging

log = logging.getLogger(__name__)

class ExecutionContext:
    """
    Manages state and connections for a specific Pipeline run.
    Replaces global registries to allow thread-safe, concurrent pipeline execution.
    """
    def __init__(self):
        self._connections: Dict[str, Resource] = {}
    
    def register_resource(self, id: str, res: Resource):
        if id in self._connections:
            raise ValueError(f"Resource '{id}' already established in this context.")
        self._connections[id] = res
    
    def get_resource(self, id: str) -> Resource:
        if id not in self._connections:
            raise KeyError(f"Unknown resource connections: {id}")
        return self._connections[id]
    
    def close_all(self):
        """Safely closes all connections tied to this execution run."""
        for res_id, conn in self._connections.items():
            try:
                conn.disconnect()
            except Exception as e:
                log.warning(f"Failed to close resource '{res_id}': {e}")
        self._connections.clear()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc, tb):
        self.close_all()