import logging
from optparse import OptionParser

from gasp.admanager import AdManager
from gasp.config import Config
from gasp.spreadsheet import Spreadsheet


def run(config_path):
    config = Config(config_path)

    admanager = AdManager(config)
    spreadsheet = Spreadsheet(config)

    from pprint import pprint

    pprint([config, admanager, spreadsheet])


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
