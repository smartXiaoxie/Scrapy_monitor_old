#coding:utf-8
import time
from common_api import WeiXinActiveApi
from pymongo.mongo_client import MongoClient
from bson.objectid import ObjectId
import dateutil.parser
import tornado.ioloop
import redis
import re
import json
import datetime

class RedisQueue(object):
    def __init__(self, name, namespace='queue', **redis_kwargs):
        self.__db = redis.Redis(**redis_kwargs)
        self.key = '%s:%s' % (namespace, name)

    def qsize(self):
        return self.__db.llen(self.key)

    def empty(self):
        return self.qsize() == 0

    def put(self, item):
        self.__db.rpush(self.key, item)

    def get(self):
        item = self.__db.lpop(self.key)
        return item


class RedisSet(object):
    def __init__(self, name):
        self.__db = redis.Redis()
        self.name = name

    def add(self, value):
        self.__db.sadd(self.name, value)

    def member(self):
        return self.__db.smembers(self.name)

    def pop(self):
        self.__db.spop(self.name)

    def delete(self, value):
        self.__db.srem(self.name, value)

class Monitor_long_Message(object):
     def __init__(self):
         self.conn = MongoClient(host="127.0.0.1", port=27017)
         self.Weixin = WeiXinActiveApi()
         self.spider_rules = RedisSet("spider_rules")
         self.period = 10000

    #检测数据的比例
     def check_data_total(self,old_data,new_data):
         if (old_data["data_total"]["total"] * 0.3) > new_data["data_total"]["total"]:
             return 3
         elif (old_data["data_total"]["total"] * 0.5) > new_data["data_total"]["total"]:
             return 2
         elif (old_data["data_total"]["total"] * 0.7) > new_data["data_total"]["total"]:
             return 1
         return 0

     # 检测空字符的比例
     def check_request_abnormal(self, new_data):
         request_dict = {}
         if "request_stats" in new_data.keys():
             for request_item, request_num in new_data["request_stats"].iteritems():
                 if request_item != "request_total" and request_item != "request_failed":
                     request_key = request_item.split('/')[-1].split('_')[-1]
                     if not new_data["request_stats"]["request_total"]:
                         request_total = 0
                     else:
                         request_total = new_data["request_stats"]["request_total"]
                     request_value = round(float(request_num) / request_total, 2)
                     request_dict[request_key] = request_value
         return request_dict

     #检查过滤项目不需要的预警
     def spider_name_filter(self, spider_name, spider_rules):
        for rule in spider_rules:
            if rule.split(":")[1] == "模糊匹配":
                pattern = re.compile(rule.split(":")[0])
                if re.findall(pattern, spider_name):
                    return True
            elif rule.split(":")[1] == "全匹配":
                if spider_name == rule.split(":")[0]:
                    return True
        return False

     #检测的主逻辑,long_spider
     def main_long_run(self):
         spider_id_list = []
         client = self.conn["info"]
         cursor = client.spiderinfocollect.find({"finish_time":None},{"_id":1})
         if cursor.count() != 0:
             for id in cursor:
                 spider_id_list.append(id["_id"])
         print spider_id_list
         for spider_id in spider_id_list:
             spider_cursor = client.spiderinfocollect.find({'_id':ObjectId(spider_id)})
             for spider in spider_cursor:
                 cursor_data = client.long_spider_alter.find({"spider_name":spider["spider_name"]})
                 if cursor_data.count() == 0:
                     client.long_spider_alter.insert({"spider_name":spider["spider_name"],
                                                      "start_time":spider["start_time"],
                                                      "record_time":datetime.datetime.now(),
                                                      "record_data":spider["data_total"],
                                                      "record_request_stats":spider["request_stats"]})
                 else:
                     for old_data in cursor_data:
                         if not (old_data["start_time"] - spider["start_time"]):
                             if (datetime.datetime.now() - old_data["record_time"]).seconds >= 3600:
                                 # 检查爬虫数据的变化
                                 self.check_data(old_data,spider)
