import logging

import yaml
from googleads.ad_manager import AdManagerClient

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class AdManager:
    @staticmethod
    def setting_yaml_string(config):
        setting = {
            "ad_manager": {
                "application_name": "admanager-setting-spreadsheet",
                "network_code": config.get("ad_manager.network_code"),
                "path_to_private_key_file": config.get("key"),
            }
        }
        return yaml.dump(setting)

    def __init__(self, config):
        self.client = AdManagerClient.LoadFromString()

    def find_or_create_order(self):
        pass

    def find_or_create_creatives(self):
        pass

    def find_or_create_lineitems(self):
        pass
