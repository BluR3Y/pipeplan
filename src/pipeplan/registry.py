from typing import Dict, Type, Callable
from abc import ABC
from importlib.metadata import entry_points
import logging
from .core import task

log = logging.getLogger(__name__)

class Operation(ABC):
    """Plugin architecture for dynamic tool registration."""
    _OPERATION_REGISTRY: Dict[str, Type["Operation"]] = {}

    def __init_subclass__(cls, type_id: str, **kwargs):
        super().__init_subclass__(**kwargs)
        if type_id:
            cls._OPERATION_REGISTRY[type_id] = cls
            cls._function_registry: Dict[str, Callable] = {}
            cls._plugins_loaded = False
    
    # Not included in gemini rework
    # @classmethod
    # def get_subclass(cls, type_id: str) -> Type["Operation"]:
    #     if type_id not in cls._OPERATION_REGISTRY:
    #         raise ValueError(f"Unknown operation type: {type_id}")
    #     return cls._OPERATION_REGISTRY[type_id]
    @classmethod
    def get_operation_type(cls, type_id: str) -> Type["Operation"]:
        if type_id not in cls._OPERATION_REGISTRY:
            raise KeyError(f"Unknown operation type: {type_id}")
        return cls._OPERATION_REGISTRY[type_id]
    
    @classmethod
    def _load_plugins(cls, type_id: str):
        # if getattr(cls, "_plugins_loaded", False):
        #     return
        if cls.__dict__.get("_plugins_loaded", False):
            return

        try:
            group_name = f"pipeplan.{type_id}"
            eps = entry_points()
            candidates = eps.select(group=group_name) if hasattr(eps, "select") else eps.get(group_name, [])

            for ep in candidates:
                try:
                    plugin_fn = ep.load()
                    if callable(plugin_fn) and ep.name not in cls._function_registry:
                        cls.register_operation(ep.name)(plugin_fn)
                    log.debug(f"Loaded '{type_id}' plugin '{ep.name}'")
                except Exception as e:
                    log.warning(f"Failed to load '{group_name}' plugin '{ep.name}': {e}")
        except Exception as e:
            log.warning(f"Plugin import failed: {e}")
        finally:
            cls._plugins_loaded = True

    @classmethod
    def register_operation(cls, name: str):
        def decorator(fn: Callable):
            # Auto-wrap registered operations in the @task decorator!
            task_wrapped_fn = task()(fn)
            cls._function_registry[name] = task_wrapped_fn
            return task_wrapped_fn
        return decorator

    @classmethod
    def get_operation(cls, name: str) -> Callable:
        # if not getattr(cls, "_plugins_loaded", False):
        #     for t_id, t_cls in cls._OPERATION_REGISTRY.items():
        #         if t_cls is cls:
        #             cls._load_plugins(t_id)
        #             break
        if cls.__dict__.get("_plugins_loaded", False):
            for t_id, t_cls in cls._OPERATION_REGISTRY.items():
                if t_cls is cls:
                    cls._load_plugins(t_id)
                    break
        
        if name not in cls._function_registry:
            raise KeyError(f"Unknown operation '{name}' in {cls.__name__}")
        return cls._function_registry[name]