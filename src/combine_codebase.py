import time
import uuid
import logging
import concurrent.futures
from typing import Dict, Callable, Any, List, Optional, Protocol, runtime_checkable, Set, Type
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
from graphlib import TopologicalSorter, CycleError
import contextvars
from contextvars import ContextVar
from abc import ABC, abstractmethod
from importlib.metadata import entry_points
import json

log = logging.getLogger(__name__)

# ==========================================
# Thread-Safe Global Contexts
# ==========================================
current_context: ContextVar["ExecutionContext"] = ContextVar("current_context")
current_pipeline: ContextVar["Pipeline"] = ContextVar("current_pipeline", default=None)

@runtime_checkable
class ResourceProtocol(Protocol):
    def connect(self) -> None: ...
    def disconnect(self) -> None: ...
    def read(self, *args, **kwargs) -> Any: ...
    def write(self, data: Any, *args, **kwargs) -> None: ...
    def __enter__(self) -> "ResourceProtocol": ...
    def __exit__(self, exc_type, exc_val, exc_tb) -> None: ...

class ExecutionContext:
    def __init__(self):
        self._connections: Dict[str, ResourceProtocol] = {}
        self._token = None
        
    def register_resource(self, id: str, resource: ResourceProtocol):
        self._connections[id] = resource
        
    def get_resource(self, id: str) -> ResourceProtocol:
        if id not in self._connections:
            raise KeyError(f"Unknown resource connection: {id}")
        return self._connections[id]

    def close_all(self):
        for res_id, conn in self._connections.items():
            try:
                conn.disconnect()
            except Exception as e:
                log.warning(f"Failed to close resource '{res_id}': {e}")
        self._connections.clear()

    def __enter__(self):
        self._token = current_context.set(self)
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close_all()
        current_context.reset(self._token)


# ==========================================
# Task State & Nodes
# ==========================================
class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

@dataclass
class TaskResult:
    status: TaskStatus = TaskStatus.PENDING
    output: Any = None
    error: Optional[Exception] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    elapsed_time: float = 0.0

@dataclass
class TaskNode:
    id: str
    func: Callable
    args: tuple
    kwargs: dict
    retries: int
    retry_delay: float
    result: TaskResult = field(default_factory=TaskResult)


# ==========================================
# The Declarative @task Decorator
# ==========================================
def task(func=None, *, retries: int = 3, retry_delay: float = 1.0):
    """
    Decorator that tracks function calls inside a Pipeline context,
    automatically building the dependency DAG.
    """
    def decorator(fn: Callable):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            pipe = current_pipeline.get()
            
            # If called outside a `with Pipeline():` block, execute eagerly
            if pipe is None:
                return fn(*args, **kwargs)
            
            # If inside a pipeline, register it as a node (Lazy execution)
            node_id = f"{fn.__name__}_{uuid.uuid4().hex[:6]}"
            node = TaskNode(
                id=node_id, func=fn, args=args, kwargs=kwargs,
                retries=retries, retry_delay=retry_delay
            )
            pipe.add_node(node)
            return node 
            
        return wrapper
    if func is None: return decorator
    return decorator(func)


# ==========================================
# Pipeline Engine (Parallel Execution)
# ==========================================
class Pipeline:
    def __init__(self, id: str, context: Optional[ExecutionContext] = None):
        self.id = id
        self.nodes: Dict[str, TaskNode] = {}
        self.context = context or ExecutionContext()
        self._token = None

    def __enter__(self):
        self._token = current_pipeline.set(self)
        return self

    def __exit__(self, exc_type, exc, tb):
        current_pipeline.reset(self._token)

    def add_node(self, node: TaskNode) -> "Pipeline":
        self.nodes[node.id] = node
        return self

    def add_task(
        self, 
        id: str, 
        func: Callable, 
        args: tuple = None, 
        kwargs: dict = None, 
        depends_on: dict = None, 
        retries: int = 3, 
        retry_delay: float = 1.0
    ) -> "Pipeline":
        """Method Chaining API for programmatic DAG construction."""
        args = args or ()
        kwargs = dict(kwargs or {})  
        depends_on = depends_on or {}

        for kwarg_name, parent_id in depends_on.items():
            if parent_id not in self.nodes:
                raise ValueError(f"Parent task '{parent_id}' not found.")
            kwargs[kwarg_name] = self.nodes[parent_id]

        node = TaskNode(
            id=id, func=func, args=args, kwargs=kwargs, 
            retries=retries, retry_delay=retry_delay
        )
        self.nodes[id] = node
        return self

    def _extract_dependencies(self, val: Any, deps: Set[str]):
        if isinstance(val, TaskNode):
            deps.add(val.id)
        elif isinstance(val, (list, tuple)):
            for v in val: self._extract_dependencies(v, deps)
        elif isinstance(val, dict):
            for v in val.values(): self._extract_dependencies(v, deps)

    def _resolve_args(self, val: Any) -> Any:
        if isinstance(val, TaskNode):
            if val.result.status != TaskStatus.COMPLETED:
                raise RuntimeError(f"Dependency {val.id} failed or was not run.")
            return val.result.output
        elif isinstance(val, list):
            return [self._resolve_args(v) for v in val]
        elif isinstance(val, tuple):
            return tuple(self._resolve_args(v) for v in val)
        elif isinstance(val, dict):
            return {k: self._resolve_args(v) for k, v in val.items()}
        return val

    def _execute_node(self, node: TaskNode):
        """Encapsulated execution logic for thread workers."""
        node.result.start_time = time.time()
        node.result.status = TaskStatus.RUNNING
        
        real_args = self._resolve_args(node.args)
        real_kwargs = self._resolve_args(node.kwargs)
        
        try:
            log.info(f"Running Task: {node.id}")
            node.result.output = node.func(*real_args, **real_kwargs)
            node.result.status = TaskStatus.COMPLETED
        except Exception as e:
            node.result.status = TaskStatus.FAILED
            node.result.error = e
            log.error(f"Task {node.id} failed: {e}")
            raise
        finally:
            node.result.end_time = time.time()
            node.result.elapsed_time = round(node.result.end_time - node.result.start_time, 4)

    def run(self, max_workers: int = 4):
        """Executes tasks topologically, utilizing parallel threads."""
        log.info(f"Executing Pipeline: {self.id}")
        
        dag: Dict[str, Set[str]] = {}
        for node_id, node in self.nodes.items():
            deps = set()
            self._extract_dependencies(node.args, deps)
            self._extract_dependencies(node.kwargs, deps)
            dag[node_id] = deps

        ts = TopologicalSorter(dag)
        try:
            ts.prepare()
        except CycleError as e:
            log.critical(f"Cyclic dependency detected: {e}")
            raise

        with self.context:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                running_tasks = {}
                
                while ts.is_active():
                    ready_nodes = ts.get_ready()
                    
                    # 1. Capture the context from the main thread
                    ctx = contextvars.copy_context()
                    
                    for node_id in ready_nodes:
                        node = self.nodes[node_id]
                        
                        # 2. Use ctx.run to inject the main thread's context into the worker thread!
                        future = executor.submit(ctx.run, self._execute_node, node)
                        running_tasks[future] = node_id
                    
                    if not running_tasks:
                        break
                        
                    done, _ = concurrent.futures.wait(
                        running_tasks.keys(), 
                        return_when=concurrent.futures.FIRST_COMPLETED
                    )
                    
                    for future in done:
                        node_id = running_tasks.pop(future)
                        future.result()  # Re-raises thread exceptions
                        ts.done(node_id)
                        
        log.info(f"Pipeline '{self.id}' execution finished.")

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
    def _load_plugins(cls, type_id: str):
        if getattr(cls, "_plugins_loaded", False):
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
        if not getattr(cls, "_plugins_loaded", False):
            for t_id, t_cls in cls._OPERATION_REGISTRY.items():
                if t_cls is cls:
                    cls._load_plugins(t_id)
                    break

        if name not in cls._function_registry:
            raise KeyError(f"Unknown operation '{name}' in {cls.__name__}")
        return cls._function_registry[name]

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

class TransferOps(Operation, type_id="transfer"): pass

@TransferOps.register_operation("extract")
def _extract(resource_id: str, as_dataframe: bool = False, *args, **kwargs) -> Any:
    """Extracts data seamlessly using the thread-safe global context."""
    
    # Notice: No context passed in the arguments! We grab it dynamically.
    context = current_context.get()
    adapter = context.get_resource(resource_id)
    
    with adapter:
        data = adapter.read(*args, **kwargs)
        
    if as_dataframe:
        import pandas as pd # Lazy load
        if isinstance(data, (list, dict)):
            data = pd.json_normalize(data)
        elif not isinstance(data, pd.DataFrame):
            data = pd.DataFrame(data)
            
    return data

class TransformOps(Operation, type_id="transform"): pass

@TransformOps.register_operation("clean_strings")
def clean_strings(df: Any, column: str, replace: str = "") -> Any:
    import pandas as pd
    df_copy = df.copy()
    if column in df_copy.columns:
        df_copy[column] = df_copy[column].astype(str).str.replace(replace, "", regex=False).str.strip()
    return df_copy