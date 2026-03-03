import logging
from typing import Any, Dict
from .operation import Operation
from .exec_context import ExecutionContext

log = logging.getLogger(__name__)

class TransferOps(Operation, type_id="transfer"):
    """Namespace for generic transfer actions (extract, load)."""
    pass

@TransferOps.register_operation("extract")
def _extract(context: ExecutionContext, resource_id: str, *args, **kwargs) -> Dict[str, Any]:
    """Extracts data from a contextual resource."""
    adapter = context.get_resource(resource_id)

    with adapter:
        data = adapter.read(*args, **kwargs)
    
    return data

@TransferOps.register_operation("load")
def _load(context: ExecutionContext, resource_id: str, data: Any, *args, **kwargs) -> None:
    adapter = context.get_resource(resource_id)

    with adapter:
        adapter.write(data=data, *args, **kwargs)