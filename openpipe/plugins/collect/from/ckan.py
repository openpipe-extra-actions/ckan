"""
# ckan
"""
from openpipe.engine import PluginRuntime
from ckanapi import RemoteCKAN, errors


class Plugin(PluginRuntime):

    __default_config__ = {
        "ignore_not_found": False
    }

    def on_start(self, config):
        self.ckan = RemoteCKAN(config['url'])

    def on_input(self, item):
        action = self.config['action']
        args = self.config.get('arguments', {})
        try:
            data = self.ckan.call_action(action, args)
        except errors.NotFound:
            if self.config['ignore_not_found']:
                return
            else:
                raise
        if isinstance(data, list):
            for new_item in data:
                self.put(new_item)
        else:
            self.put(data)
