import logging

import googleapiclient.discovery as discovery
from google.oauth2 import service_account
from jsonschema import validate

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# jsonschema parts
non_empty_string = {"type": "string", "minLength": 1}
positive_integer = {"type": "integer", "minimum": 1}


class Spreadsheet:

    SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

    def __init__(self, config):
        credentials = service_account.Credentials.from_service_account_file(config.get("key"), scopes=self.SCOPES)
        service = discovery.build("sheets", "v4", credentials=credentials)

        self.config = config
        self.api = service.spreadsheets()
        self.getopts = {
            "spreadsheetId": config.get("spreadsheet.id"),
            "majorDimension": "ROWS",
            "valueRenderOption": "UNFORMATTED_VALUE",
        }
        self.cache = {}

    def fetch_rows(self, sheet_type):
        if sheet_type in self.cache:
            return self.cache[sheet_type]

        sheet_config = self.config.get_or_default(f"spreadsheet.sheets.{sheet_type}")
        assert sheet_type in self.config.get_or_default("spreadsheet.sheets").keys()

        rows = []

        hasNext, page, rows_per_req = True, 0, 1000
        while hasNext:
            start, end = page * rows_per_req + 1, (page + 1) * rows_per_req
            range = f"'{sheet_config['sheet_name']}'!A{start}:R{end}"

            logger.info(f"fetching a sheet: {range}")
            response = self.api.values().get(range=range, **self.getopts).execute()  # check failure
            rows.extend(response["values"])
            page += 1
            if len(response["values"]) < rows_per_req:
                hasNext = False

        # filter empty & comment
        rows = list(filter(lambda r: len(r) != 0 and not r[0].startswith("#"), rows))

        # rows to dict
        header, mapping = rows.pop(0), {}
        for key, name in sheet_config["columns"].items():
            mapping[key] = header.index(name)
        models = map(lambda r: {k: r[idx] if idx < len(r) else "" for k, idx in mapping.items()}, rows)

        self.cache[sheet_type] = list(models)
        return self.cache[sheet_type]

    def check_settings(self):
        logger.info(f"check settings in the spreadsheet")

        orders = self.fetch_rows("order")
        self.check_orders(orders)
        lineitems = self.fetch_rows("lineitem")
        self.check_lineitems(lineitems, orders)
        creatives = self.fetch_rows("creative")
        self.check_creatives(creatives, orders, lineitems)

    def check_orders(self, orders):
        # check values
        schema = {
            "type": "object",
            "properties": {
                "name": non_empty_string,
                "advertiser_name": non_empty_string,
                "trafficker_name": non_empty_string,
            },
            "required": ["name", "advertiser_name", "trafficker_name"],
        }
        logger.info(f"checking order settings")
        [validate(o, schema) for o in orders]

        # name must be unique
        names = list(map(lambda o: o["name"], orders))
        assert len(set(names)) == len(names), "order name must be unique"

    def check_lineitems(self, lineitems, orders):
        order_names = list(set(list(map(lambda o: o["name"], orders))))

        single_keyvalue = {"type": "string", "pattern": "^(.+=.+)?$"}

        schema = {
            "type": "object",
            "properties": {
                "order_name": {"type": "string", "enum": order_names},
                "name": non_empty_string,
                "sizes": {"type": "string", "minLength": 1, "pattern": "^\\d+x\\d+(\\s?,\\s?\\d+x\\d+)*$"},
                "costPerUnit": positive_integer,
                "targetingUnit": positive_integer,
                **dict(map(lambda n: ("targetingKeyValue" + str(n), single_keyvalue), range(1, 12 + 1))),
            },
            "required": ["order_name", "name", "sizes", "costPerUnit", "targetingUnit"],
        }
        logger.info(f"checking lineitem settings")
        [validate(l, schema) for l in lineitems]

        # name must be unique in order
        names = list(map(lambda li: li["name"] + li["order_name"], lineitems))
        assert len(set(names)) == len(names), "lineitem name must be unique in order"

    def check_creatives(self, creatives, orders, lineitems):
        order_names = list(set(list(map(lambda o: o["name"], orders))))
        lineitem_names = list(set(list(map(lambda li: li["name"], lineitems))))

        schema = {
            "type": "object",
            "properties": {
                "order_name": {"type": "string", "enum": order_names},
                "lineitem_name": {"type": "string", "enum": lineitem_names},
                "name": non_empty_string,
                "snippet": non_empty_string,
            },
        }
        logger.info(f"checking creative settings")
        [validate(c, schema) for c in creatives]

        # name must be unique
        names = list(map(lambda c: c["name"], creatives))
        assert len(set(names)) == len(names), "creative name must be unique"
