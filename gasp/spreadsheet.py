import logging

import googleapiclient.discovery as discovery
from google.oauth2 import service_account

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


# values = sheet.values().get(spreadsheetId=config.get('spreadsheet.id'), range="Order").execute()


class Spreadsheet:

    SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

    def __init__(self, config):
        credentials = service_account.Credentials.from_service_account_file(config.get("key"), scopes=self.SCOPES)
        service = discovery.build("sheets", "v4", credentials=credentials)

        self.config = config
        self.api = service.spreadsheets()
        self.spreadsheetId = config.get("spreadsheet.id")

    def check_configs(self):
        pass
