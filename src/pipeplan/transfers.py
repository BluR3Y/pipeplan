from typing import Any
from .registry import Operation
from .core import current_context

class TransferOps(Operation, type_id="transfer"): pass

@TransferOps.register_operation("extract")
def extract(resource_id: str, as_dataframe: bool = False, *args, **kwargs) -> Any:
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

# @TransferOps.register_operation("load")
# def _load(resource_id: str, data: Any, *args, **kwargs) -> None:
#     ...