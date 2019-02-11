"""
# ckan
"""
from openpipe.engine import PluginRuntime
from ckanapi import RemoteCKAN


class Plugin(PluginRuntime):

    def on_start(self, config):
        self.ckan = RemoteCKAN(config['url'])

    def on_input(self, item):
        action = self.config['action']
        args = self.config.get('arguments', {})
        data = self.ckan.call_action(action, args)
        if isinstance(data, list):
            for new_item in data:
                self.put(new_item)
        else:
            self.put(data)
