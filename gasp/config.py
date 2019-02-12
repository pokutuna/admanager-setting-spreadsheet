import json
import os

_root = os.path.normpath(os.path.dirname(os.path.abspath(__file__)) + "/..")


class Config:

    defaults = {}

    def __init__(self, path):
        self.path = path
        with open(path) as f:
            self.data = json.load(f)

    def get(self, path):
        return get_field(path, self.data)

    def get_or_default(self, path):
        try:
            return self.get(path)
        except KeyError:
            return get_field(path, self.defaults)


def get_field(path, config):
    ary = path.split(".")
    paths, key = ary[:-1], ary[-1:][0]
    for p in paths:
        if p in config and isinstance(config[p], dict):
            config = config[p]
        else:
            raise KeyError()
    return config[key]
