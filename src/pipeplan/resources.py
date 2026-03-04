import json
import logging
from typing import Any
from .registry import Operation

log = logging.getLogger(__name__)

class ResourceOps(Operation, type_id="resource"):
    @classmethod
    def register_operation(cls, name: str):
        """
        Override the default registry behavior.
        Resources are classes, Not operations, so they should not be wrapped in @task.
        """
        def decorator(resource_class):
            cls._function_registry[name] = resource_class
            return resource_class
        return decorator

class FileResource(ResourceOps, type_id="file"): pass
class DatabaseResource(ResourceOps, type_id="db"): pass
class APIResource(ResourceOps, type_id="api"): pass

@FileResource.register_operation("json")
class JsonAdapter:
    def __init__(self, path: str, mode: str = "r"):
        self.path = path
        self.mode = mode
        self.client = None

    def connect(self):
        self.client = open(self.path, self.mode)
        log.debug(f"Connected to Json File: {self.path}")

    def disconnect(self):
        if self.client and not self.client.closed:
            self.client.close()
            log.debug(f"Closed Json File: {self.path}")
    
    def read(self, *args, **kwargs) -> Any:
        return json.load(self.client)
    
    def write(self, data: Any, *args, **kwargs) -> None:
        json.dump(data, self.client, indent=4)

    # --- Context Manager Protocol ---
    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()