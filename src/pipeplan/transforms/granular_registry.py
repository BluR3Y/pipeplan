import logging
from typing import Any, Dict
from ..operation import Operation

log = logging.getLogger(__name__)

class TransformOps(Operation, type_id="transform"):
    def __init_subclass__(cls, type_id, **kwargs):
        return super().__init_subclass__(type_id=f"transform.{type_id}", **kwargs)

class ElementTransform(TransformOps, type_id="element"): pass
class SetTransform(TransformOps, type_id="set"): pass
class CollectionTransform(TransformOps, type_id="collection"): pass