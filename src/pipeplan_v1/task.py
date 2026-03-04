import time
import logging
import inspect
from enum import Enum
from typing import Callable, Dict, Any, Optional, Tuple, Type
from .exec_context import ExecutionContext

log = logging.getLogger(__name__)

class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

class Task:
    def __init__(
        self,
        id: str,
        action: Callable,
        depends_on: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        attempts: int = 3,
        retry_delay: float = 1.0,
        backoff_factor: float = 2.0,
        retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,),
        metadata: Optional[Dict] = None
    ):
        self.id = id
        self.action = action
        self.depends_on = depends_on or {}
        self.params = params or {}

        self.attempts = attempts
        self.retry_delay = retry_delay
        self.backoff_factor = backoff_factor
        self.retryable_exceptions = retryable_exceptions
        self.metadata = metadata or {}

        self.reset_state()
    
    def reset_state(self):
        """Clear state from previous runs to allow safe retries/reruns."""
        self.status = TaskStatus.PENDING
        self.output: Any = None
        self.error: Optional[Exception] = None
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.elapsed_time: float = 0.0
    
    def run(self, context: ExecutionContext, **dependency_kwargs) -> Any:
        """Execution Wrapper handling task lifecycle with exponential backoff."""
        self.reset_state()
        self.status = TaskStatus.RUNNING
        self.start_time = time.time()
        log.info(f"Task '{self.id}' started.")

        merged_kwargs = dict(**self.params, **dependency_kwargs)

        # Dependency Injection: Inject Context if the action asks for it
        sig = inspect.signature(self.action)
        if "context" in sig.parameters:
            merged_kwargs["context"] = context
        
        remaining_attempts = self.attempts
        current_delay = self.retry_delay

        while remaining_attempts > 0:
            try:
                log.debug(f"Running '{self.id}' with kwargs: {list(merged_kwargs.keys())}")
                self.output = self.action(**merged_kwargs)
                self.status = TaskStatus.COMPLETED
                break

            except Exception as e:
                if not isinstance(e, self.retryable_exceptions):
                    log.error(f"Task '{self.id}' encountered non-retryable error: {e}")
                    self.status = TaskStatus.FAILED
                    self.error = e
                    self._finalize_metrics()
                    raise e
                
                remaining_attempts -= 1
                if remaining_attempts == 0:
                    self.status = TaskStatus.FAILED
                    self.error = e
                    self._finalize_metrics()
                    log.error(f"Task '{self.id}' failed permanently. Error: {e}", exc_info=True)
                    raise e
                else:
                    log.warning(f"Task '{self.id}' failed. Retrying in {current_delay}s. Error: {e}")
                    current_delay *= self.backoff_factor
        
        self._finalize_metrics()
        log.info(f"Task '{self.id}' finished in {self.elapsed_time}s with status: {self.status.value}")
        return self.output

    def _finalize_metrics(self):
        self.end_time = time.time()
        if self.start_time:
            self.elapsed_time = round(self.end_time - self.start_time, 4)
    
    def __repr__(self):
        return f"<Task id={self.id} status={self.status.value}>"