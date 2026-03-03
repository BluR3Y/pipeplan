import logging
import json
from ..adapter_registry import FileResource
from ..base_cls import Resource

log = logging.getLogger(__name__)

@FileResource.register_operation("json")
class JsonAdapter(Resource):
    def connect(self):
        file_path = self.cfg.get("path")
        mode = self.cfg.get("mode", "r")
        self.client = open(file_path, mode)
        log.debug(f"Connected to Json File: {file_path} in mode '{mode}")

    def disconnect(self):
        if self.client and not self.client.closed:
            self.client.close()
    
    def read(self, *args, **kwargs):
        return json.load(self.client)
    
    def write(self, data, *args, **kwargs):
        json.dump(data, self.client, indent=4)