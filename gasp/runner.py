import logging
from optparse import OptionParser

from gasp.admanager import AdManager
from gasp.config import Config
from gasp.spreadsheet import Spreadsheet


def run(config_path):
    config = Config(config_path)

    admanager = AdManager(config)
    spreadsheet = Spreadsheet(config)

    spreadsheet.check_settings()

    admanager.setup_lineitems(
        order_rows=spreadsheet.fetch_rows("order"), lineitem_rows=spreadsheet.fetch_rows("lineitem")
    )
    admanager.setup_creatives(
        creative_rows=spreadsheet.fetch_rows("creative"),
        order_rows=spreadsheet.fetch_rows("order"),
        lineitem_rows=spreadsheet.fetch_rows("lineitem"),
    )
    admanager.setup_lineitemassociation(
        order_rows=spreadsheet.fetch_rows("order"),
        lineitem_rows=spreadsheet.fetch_rows("lineitem"),
        creative_rows=spreadsheet.fetch_rows("creative"),
    )


if __name__ == "__main__":
    logging.getLogger("gasp").setLevel(level=logging.INFO)
    logging.getLogger("gasp").addHandler(logging.StreamHandler())

    parser = OptionParser()
    parser.add_option("--config", type="string", dest="config_path")
    options, _ = parser.parse_args()
    if options.config_path is None:
        parser.print_help()
        exit()

    run(options.config_path)
