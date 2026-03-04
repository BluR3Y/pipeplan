from abc import ABC, abstractmethod
from typing import Any

class Resource(ABC):
    def __init__(self, **kwargs):
        self.cfg = kwargs
        self.client = None
    
    @abstractmethod
    def connect(self): pass

    @abstractmethod
    def disconnect(self): pass

    @abstractmethod
    def read(self, *args, **kwargs) -> Any: pass

    @abstractmethod
    def write(self, data: Any, *args, **kwargs) -> None: pass

    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc, tb):
        self.disconnect()