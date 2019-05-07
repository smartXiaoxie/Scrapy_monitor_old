# coding=utf-8
import time
import datetime
from common_api import WeiXinActiveApi
from pymongo.mongo_client import MongoClient
import redis
import re
import json


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


class MonitorMessage(object):
    
    def __init__(self):
        self.conn = MongoClient(host="127.0.0.1", port=27017)
        #self.spider_info_dict = {}
        self.task_queue = RedisQueue("message")
        self.weixin = WeiXinActiveApi()
        self.spider_rules = RedisSet("spider_rules")

    def check_data_total(self, old_data, new_data):
        if (old_data["data_total"]["total"] * 0.3) > new_data["data_total"]["total"]:
            return 3
        elif (old_data["data_total"]["total"] * 0.5) > new_data["data_total"]["total"]:
            return 2
        elif (old_data["data_total"]["total"] * 0.7) > new_data["data_total"]["total"]:
            return 1
        return 0

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

    def check_data(self, old_data, new_data, spider_name):
        token = self.weixin.get_exists_token("liusize")
        flag = self.check_data_total(old_data, new_data)
        alert_flag = self.spider_name_filter(spider_name, self.spider_rules.member())
        if (not alert_flag) and flag == 1:
            self.weixin.push_message(token, msgtype="text", touser="@all", agentid=3, content=new_data["spider_name"] + u": 数据总量减少30%")
        elif (not alert_flag) and flag == 2:
            self.weixin.push_message(token, msgtype="text", touser="@all", agentid=3, content=new_data["spider_name"] + u": 数据总量减少50%")
        elif (not alert_flag) and flag == 3:
            self.weixin.push_message(token, msgtype="text", touser="@all", agentid=3, content=new_data["spider_name"] + u": 数据总量减少70%")
        else:
            #old_data[spider_name] = new_data
            self.conn.info.alter_info.remove({"spider_name":spider_name})
            print "remove secusseeful"
            self.conn.info.alter_info.insert(new_data)
            print "*** new_data %s" % new_data

    def run(self):
        while True:
            qsize = self.task_queue.qsize()
            print "redis size %s" % qsize
            if qsize:
                for i in range(qsize):
                    spider_info = self.task_queue.get()
                    spider_info = json.loads(spider_info.strip())
                    print "spider_info %s" % spider_info
                    # 将存放到内存中的数据存放数据库中
                    client = self.conn["info"]
                    cursor = client.alter_info.find({"spider_name":spider_info["spider_name"]})
                    if cursor.count() == 0:
                        client.alter_info.insert(spider_info)
                    cursor = client.alter_info.find({"spider_name":spider_info["spider_name"]})
                    for alter_data in cursor:
                        print " mongo alter_data %s" % alter_data
                        self.check_data(alter_data,spider_info,spider_info["spider_name"])
                    
                    #if spider_info["spider_name"] not in self.spider_info_dict:
                        #self.spider_info_dict[spider_info["spider_name"]] = spider_info.copy()
                    #self.check_data(self.spider_info_dict, spider_info, spider_info["spider_name"])
                    #print spider_info, self.spider_info_dict[spider_info["spider_name"]]
                    #db = self.conn["info"]["spiderinfocollecthistory"]
                    spider_info["start_time"] = datetime.datetime.strptime(spider_info["start_time"], '%Y-%m-%d %H:%M:%S')
                    spider_info["finish_time"] = datetime.datetime.strptime(spider_info["finish_time"], '%Y-%m-%d %H:%M:%S')

                    client.spiderinfocollecthistory.insert(spider_info)
            time.sleep(15)
                
if __name__ == "__main__":
    temp = MonitorMessage()
    temp.run()
