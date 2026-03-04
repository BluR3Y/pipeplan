import logging
from .task import Task
from .exec_context import ExecutionContext
from typing import Any, Dict, List, Type, Optional
from graphlib import TopologicalSorter, CycleError

log = logging.getLogger(__name__)

class Pipeline:

    def __init__(self, id: str, context: Optional[ExecutionContext] = None):
        self.id = id
        self.tasks: Dict[str, Task] = {}
        self._task_dag: Dict[str, List[str]] = {}
        self.context = context or ExecutionContext()
    
    def add_task(self, task: Task):
        """Register a task and computes its upstream dependency graph."""
        self.tasks[task.id] = task
        # Extract parent task IDs from the dependency mapping
        parent_ids = list(task.depends_on.values())
        self._task_dag[task.id] = list(set(parent_ids))
    
    def run(self):
        """Executes tasks in the correct topological order within the context."""
        log.info(f"Starting Pipeline: {self.id}")

        all_deps = {dep for deps in self._task_dag.values() for dep in deps}
        missing_deps = all_deps - set(self.tasks.keys())
        if missing_deps:
            raise ValueError(f"Tasks {missing_deps} are dependencies but were never added.")
        
        ts = TopologicalSorter(self._task_dag)
        try:
            execution_order = tuple(ts.static_order())
        except CycleError as e:
            log.critical(f"Pipeline validation failed: Cyclic dependency detected. {e}")
            raise

        with self.context:
            for task_id in execution_order:
                try:
                    self._execute_task(task_id)
                except Exception as e:
                    log.critical(f"Pipeline halted at task '{task_id}' due to: {e}")
                    raise
        log.info(f"Pipeline '{self.id}' completed successfully.")
    
    def _execute_task(self, task_id: str):
        task = self.tasks[task_id]

        dep_kwargs = {
            kwarg_name: self.tasks[parent_task_id].output
            for kwarg_name, parent_task_id in task.depends_on.items()
        }
        task.run(context=self.context, **dep_kwargs)