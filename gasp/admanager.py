import functools
import logging
from pprint import pformat
from time import sleep

import googleads.ad_manager as ad_manager
import yaml
import zeep
from more_itertools import chunked

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

API_VERSION = "v202008"


def memoize(obj):
    cache = obj.cache = {}

    @functools.wraps(obj)
    def memoizer(*args):
        if args not in cache:
            cache[args] = obj(*args)
        return cache[args]

    return memoizer


def compare_objects(key, settings: list, existing: list, key_only=False):
    """
    compare settings to existing by key
    and returns
    {
        notfound: [...],
        different: [...],
        existing: [...],
    }
    """
    returns = {"notfound": [], "different": [], "existing": []}
    existing_map = {e[key]: e for e in existing}
    for s in settings:
        rel = s[key]
        if rel in existing_map:
            if key_only is True or is_containing(s, existing_map[rel]):
                returns["existing"].append(s)
            else:
                returns["different"].append(s)
        else:
            returns["notfound"].append(s)
    return returns


def is_containing(setting, existing):
    se = setting.copy()
    se.pop("xsi_type", None)  # not containing serialized object
    native_object = zeep.helpers.serialize_object(existing)
    return se.items() <= native_object.items()


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
        admanager_setting = self.setting_yaml_string(config)
        self.currency_code = config.get("ad_manager.currency_code")
        self.client = ad_manager.AdManagerClient.LoadFromString(admanager_setting)

    @memoize
    def find_one(self, service_name, method, *args):
        service = self.client.GetService(service_name, version=API_VERSION)
        query = ad_manager.StatementBuilder()
        if len(args) % 2 != 0:
            raise Exception("args must be key value pair")

        q = " AND ".join([f"{k} = :{k}" for k, v in chunked(args, 2)])
        query.Where(q)
        [query.WithBindVariable(k, v) for k, v in chunked(args, 2)]
        response = getattr(service, method)(query.ToStatement())
        assert "results" in response
        if len(response["results"]) == 1:
            return response["results"][0]
        else:
            raise ObjectNotFound(f"object not found: f{pformat(*args)}")

    def find_multi(self, service_name, method, key, values):
        service = self.client.GetService(service_name, version=API_VERSION)
        query = ad_manager.StatementBuilder(limit=None, offset=None)
        query.Where(f"{key} IN (:values)").WithBindVariable("values", values)
        response = getattr(service, method)(query.ToStatement())
        assert "results" in response
        return response["results"]

    def handle_compare_result(self, object_name, settings, result):
        if len(result["different"]) != 0:
            raise ExistingDifferentObject(pformat(result["different"]))
        if len(result["existing"]) != 0:
            logger.info(f"{object_name}: existing {len(result['existing'])}/{len(settings)} objects")

    def find_advertiser(self, name):
        return self.find_one("CompanyService", "getCompaniesByStatement", "name", name)

    def find_trafficker(self, name):
        return self.find_one("UserService", "getUsersByStatement", "name", name)

    def find_order(self, name):
        return self.find_one("OrderService", "getOrdersByStatement", "name", name)

    def find_key(self, key_name):
        return self.find_one("CustomTargetingService", "getCustomTargetingKeysByStatement", "name", key_name)

    def find_key_value(self, key_name, value_name):
        key = self.find_key(key_name)
        value = self.find_one(
            "CustomTargetingService",
            "getCustomTargetingValuesByStatement",
            "customTargetingKeyId",
            key["id"],
            "name",
            value_name,
        )
        return key, value

    def search_lineitems(self, order_id, names):
        service = self.client.GetService("LineItemService", version=API_VERSION)
        query = ad_manager.StatementBuilder()
        query.Where("orderId = :order_id AND name IN (:names)").WithBindVariable("order_id", order_id).WithBindVariable(
            "names", names
        )
        response = service.getLineItemsByStatement(query.ToStatement())
        assert "results" in response
        return response["results"]

    def setup_orders(self, order_rows):
        settings = []
        for row in order_rows:
            advertiser = self.find_advertiser(row["advertiser_name"])
            trafficker = self.find_trafficker(row["trafficker_name"])
            settings.append({"name": row["name"], "advertiserId": advertiser["id"], "traffickerId": trafficker["id"]})

        names = list(map(lambda o: o["name"], settings))
        existing = self.find_multi("OrderService", "getOrdersByStatement", "name", names)
        result = compare_objects("name", settings, existing)
        self.handle_compare_result("orders", settings, result)
        service = self.client.GetService("OrderService", version=API_VERSION)
        if 0 < len(result["notfound"]):
            service.createOrders(result["notfound"])

    def setup_creatives(self, creative_rows=[], order_rows=[], lineitem_rows=[]):
        order_to_advertiser = {o["name"]: o["advertiser_name"] for o in order_rows}
        lineitem_to_sizes = {li["name"]: li["sizes"] for li in lineitem_rows}

        blocks = list(chunked(creative_rows, 30))
        for i, rows in enumerate(blocks):
            logger.info(f"creatives: checking ({i+1}/{len(blocks)})")
            settings = []
            for row in rows:
                advertiser = self.find_advertiser(order_to_advertiser[row["order_name"]])
                lineitem_size = lineitem_to_sizes[row["lineitem_name"]]
                size = dict(zip(["width", "height"], map(int, lineitem_size.split("x"))))
                settings.append(
                    {
                        "xsi_type": "ThirdPartyCreative",
                        "name": row["name"],
                        "advertiserId": advertiser["id"],
                        "size": {**size, "isAspectRatio": False},
                        "snippet": row["snippet"],
                        "isSafeFrameCompatible": True,  # TODO configurable
                    }
                )

            names = list(map(lambda o: o["name"], settings))
            existing = self.find_multi("CreativeService", "getCreativesByStatement", "name", names)
            result = compare_objects("name", settings, existing)
            self.handle_compare_result("creatives", settings, result)
            if 0 < len(result["notfound"]):
                service = self.client.GetService("CreativeService", version=API_VERSION)
                service.createCreatives(result["notfound"])
            sleep(1)

    def setup_lineitems(self, order_rows=[], lineitem_rows=[]):
        # Process for each order because uniqueness is in (order.name, lineitem.name) pairs
        for order_row in order_rows:
            order = self.find_order(order_row["name"])
            lineitem_rows_in_order = list(filter(lambda r: r["order_name"] == order_row["name"], lineitem_rows))
            self.__setup_lineitem_in_order(order, lineitem_rows_in_order)

    def setup_lineitemassociation(self, order_rows=[], lineitem_rows=[], creative_rows=[]):
        for order_row in order_rows:
            logger.info(f'lineitem creative associations in order {order_row["name"]}')
            order = self.find_order(order_row["name"])
            lineitems = self.find_multi("LineItemService", "getLineItemsByStatement", "orderId", order["id"])

            creative_rows_in_order = list(filter(lambda r: r["order_name"] == order_row["name"], creative_rows))
            creatives = self.find_multi(
                "CreativeService",
                "getCreativesByStatement",
                "name",
                list(map(lambda r: r["name"], creative_rows_in_order)),
            )

            settings = []
            for row in creative_rows_in_order:
                lineitem = next(filter(lambda l: l["name"] == row["lineitem_name"], lineitems))
                creative = next(filter(lambda c: c["name"] == row["name"], creatives))
                settings.append({"lineItemId": lineitem["id"], "creativeId": creative["id"]})

            existing = self.find_multi(
                "LineItemCreativeAssociationService",
                "getLineItemCreativeAssociationsByStatement",
                "lineItemId",
                list(map(lambda l: l["id"], lineitems)),
            )

            to_creates = list(
                filter(
                    lambda s: not any(
                        (e["lineItemId"] == s["lineItemId"] and e["creativeId"] == s["creativeId"] for e in existing)
                    ),
                    settings,
                )
            )

            service = self.client.GetService("LineItemCreativeAssociationService", version=API_VERSION)
            blocks = chunked(to_creates, 20)
            for i, block in enumerate(blocks):
                service.createLineItemCreativeAssociations(to_creates)

    def __setup_lineitem_in_order(self, order, lineitem_rows):
        logger.info(f'lineitem settings in order {order["name"]}')
        settings = []
        for row in lineitem_rows:
            config = self.generate_lineitem_config(row)
            settings.append(config)

        blocks = list(chunked(settings, 20))
        for i, block in enumerate(blocks):
            logger.info(f"lineitems: checking ({i+1}/{len(blocks)})")

            names = list(map(lambda l: l["name"], settings))
            existing = self.search_lineitems(order["id"], names)
            result = compare_objects("name", settings, existing, key_only=True)
            self.handle_compare_result("lineitem", settings, result)
            if 0 < len(result["notfound"]):
                service = self.client.GetService("LineItemService", version=API_VERSION)
                service.createLineItems(result["notfound"])
            sleep(1)

    def generate_lineitem_config(self, row):
        order = self.find_order(row["order_name"])
        size = dict(zip(["width", "height"], map(int, row["sizes"].split("x"))))

        columns = list(map(lambda n: "targetingKeyValue" + str(n), range(1, 12 + 1)))
        criterias = list(map(lambda c: self.keyvalue_to_criteria(row[c]), filter(lambda c: row[c] != "", columns)))
        ad_units = list(map(lambda id: {"adUnitId": id}, row["targetingUnit"].split(",")))
        custom_targeting = {"xsi_type": "CustomCriteriaSet", "logicalOperator": "OR", "children": criterias}

        return {
            "orderId": order["id"],
            "name": row["name"],
            "startDateTimeType": "IMMEDIATELY",
            "unlimitedEndDateTime": True,
            "creativeRotationType": "EVEN",  # 均等
            "lineItemType": "PRICE_PRIORITY",  # 価格優先
            "costPerUnit": {"currencyCode": self.currency_code, "microAmount": int(row["costPerUnit"] * 1_000_000)},
            "costType": "CPM",
            "creativePlaceholders": [{"size": size}],
            "primaryGoal": {"goalType": "NONE"},
            "targeting": {
                "inventoryTargeting": {"targetedAdUnits": ad_units},
                "customTargeting": custom_targeting,
            },
        }

    def keyvalue_to_criteria(self, keyvalue):
        """
        "hoge=fuga" のような入力からターゲティング設定に利用する criteai dict を返す
        """
        if "!=" in keyvalue:
            key, value = self.find_key_value(*keyvalue.split("!="))
            return {"xsi_type": "CustomCriteria", "keyId": key["id"], "valueIds": [value["id"]], "operator": "IS_NOT"}
        elif "=" in keyvalue:
            key, value = self.find_key_value(*keyvalue.split("="))
            return {"xsi_type": "CustomCriteria", "keyId": key["id"], "valueIds": [value["id"]], "operator": "IS"}
        else:
            raise f"Unsupported format: {keyvalue}"


class GaspException(Exception):
    "The base exception class in this tool."


class ObjectNotFound(GaspException):
    "Object not found."


class ExistingDifferentObject(GaspException):
    "There are object which has differences with creatings."
