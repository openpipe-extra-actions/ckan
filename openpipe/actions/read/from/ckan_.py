"""
# Get data from a CKAN repository
"""
from openpipe.pipeline.engine import ActionRuntime
from ckanapi import RemoteCKAN, errors


class Action(ActionRuntime):

    required_config = """
        url:     # URL for the CKAN endpoint
        action:  # CKAN action to perform
    """

    optional_config = """
        ignore_not_found: False     # Ignore collection not found errors
    """

    def on_start(self, config):
        self.ckan = RemoteCKAN(config["url"])

    def on_input(self, item):
        action = self.config["action"]
        args = self.config.get("arguments", {})
        try:
            data = self.ckan.call_action(action, args)
        except errors.NotFound:
            if self.config["ignore_not_found"]:
                return
            else:
                raise
        if isinstance(data, list):
            for new_item in data:
                self.put(new_item)
        else:
            self.put(data)
