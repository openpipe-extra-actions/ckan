"""
# ckan
"""
from openpipe.engine import PluginRuntime
from ckanapi import RemoteCKAN


class Plugin(PluginRuntime):

    def on_start(self, config):
        self.ckan = RemoteCKAN(config['url'])

    def on_input(self, item):
        data = self.ckan.call_action(self.config['action'])
        for new_item in data:
            self.put(new_item)
