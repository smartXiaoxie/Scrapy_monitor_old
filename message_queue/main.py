# coding=utf-8
import time
import datetime
from common_api import WeiXinActiveApi
from common_dingding import DingdingActiveApi
from pymongo.mongo_client import MongoClient
import redis
import re
import json
import sys
sys.setdefaultencoding('utf-8')


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
        # self.spider_info_dict = {}
        self.task_queue = RedisQueue("message")
        self.weixin = WeiXinActiveApi()
        self.dingding = DingdingActiveApi()
        self.spider_rules = RedisSet("spider_rules")
        '''self.filter_name_dict = {
           
            }'''
        # 这里写上项目名称
        #写上对应负责人的号码，用于钉钉@ 使用，以后需要改放到数据库中
        self.mobile_dict = {}

    def check_data_total(self, old_data, new_data):
        if (old_data["data_total"]["total"] * 0.3) > new_data["data_total"]["total"]:
            return 3
        elif (old_data["data_total"]["total"] * 0.5) > new_data["data_total"]["total"]:
            return 2
        elif (old_data["data_total"]["total"] * 0.7) > new_data["data_total"]["total"]:
            return 1
        return 0

    # 检查项目的数据的异常预警
    def check_data_abnormal(self,new_data):
        if (new_data["data_total"]["total"] * 0.7) < new_data["data_total"]["abnormal"]:
            return 3
        elif (new_data["data_total"]["total"] * 0.5) < new_data["data_total"]["abnormal"]:
            return 2
        elif (new_data["data_total"]["total"] * 0.3) < new_data["data_total"]["abnormal"]:
            return 1
        return 0

    # 检查项目请求状态的预警
    def check_request_abnormal(self,new_data):
        request_dict = {}
        if "request_stats" in new_data.keys():
            for request_item,request_num in new_data["request_stats"].iteritems():
                if request_item != "request_total" and request_item != "request_failed":
                    request_key = request_item.split('/')[-1].split('_')[-1]
                    if not new_data["request_stats"]["request_total"]:
                        request_total = 0
                    else:
                        request_total = new_data["request_stats"]["request_total"]
                    request_value = round(float(request_num) / request_total, 2)
                    request_dict[request_key] = request_value
        return request_dict

    # 检查过滤项目不需要的预警
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

    # 进行预警过滤
    def alert_filter(self,alert_client,spider_name,alert_item):
        day = datetime.date.today()
        today = day.strftime("%Y-%m-%d")
        print today
        alert_data = alert_client.find({"date":today,"spider_name":spider_name})
        if alert_data.count() == 0:
            try:
                alert_client.insert({"date":today,"spider_name":spider_name,"alert_data":{alert_item:1}})
            except:
                print "预警插入数据失败"
            return True
        else:
            for alert in alert_data:
                if alert_item in alert["alert_data"].keys():
                    alert_num = alert["alert_data"][alert_item] + 1
                    print alert_num
                    item = "alert_data" + '.' + alert_item
                    print item
                    alert_client.update({"date":today,"spider_name":spider_name},{"$set":{item:alert_num}})
                    print type(alert_num)
                    #bin_alert_num = int(bin(alert_num).replace('0b',''))
                    #print bin_alert_num
                    #result = bin_alert_num & (bin_alert_num-1)
                    result = alert_num & (alert_num - 1)
                    print result
                    if result == 0:
                        print "yes"
                        return True
                    else:
                        print "no"
                        return False
                else:
                    alert_data_dict = alert["alert_data"]
                    alert_data_dict[alert_item] = 1
                    print alert_data_dict
                    try:
                        alert_client.update({"date": today, "spider_name": spider_name},{"$set": {"alert_data": alert_data_dict}})
                    except:
                        pass
                    return True

    # 解除预警的选项
    def relive_alert(self,alert_client,spider_name,alert_item):
        day = datetime.date.today()
        today = day.strftime("%Y-%m-%d")
        print today
        alert_data = alert_client.find({"date": today, "spider_name": spider_name})
        if alert_data.count() != 0:
            for data in alert_data:
                alert_data_dict = data["alert_data"]
                print alert_data_dict
                for alert_keys in  alert_data_dict.keys():
                    if alert_item in alert_keys:
                        del alert_data_dict[alert_keys]
                        print alert_data_dict
                try:
                    alert_client.update({"date":today,"spider_name":spider_name},{"$set":{"alert_data":alert_data_dict}})
                except:
                    pass

    # 进行添加负责人
    def add_handler_filter(self,spider_name):
        stat = 0
        for key,value in self.filter_name_dict.items():
            filter_name_str = ''.join(value)
            if spider_name in filter_name_str:
                handler = key
                stat = stat + 1
                break
        if stat == 0:
            handler = ""  # 写上默认的名字
        print handler 
        return handler

    # 过滤用户的手机号码，用于 钉钉 @ 功能
    def filter_mobile(self,handler):
        if handler in self.mobile_dict.keys():
            mobile = self.mobile_dict[handler]
        else:
            mobile = ""  # 写上默认的电话
        return mobile

    def check_data(self, old_data, new_data, spider_name):
        token = self.weixin.get_exists_token("")
        url="" #钉钉的url
        flag = self.check_data_total(old_data, new_data)
        # 添加负责人选项
        handler = self.add_handler_filter(spider_name)
        print "******handler*******%s" % handler
        handler_name = '负责人:  ' + handler
        print "**********handler_name*********%s" % handler_name
        mobile_num = self.filter_mobile(handler)
        print mobile_num
        alert_client = self.conn.info["warning_display"]
        # 检查新数据的总数是旧数据总数的比例
        alert_flag = self.spider_name_filter(spider_name, self.spider_rules.member())
        # 检查项目是否在不需要的预警队列中
        if (not alert_flag) and flag == 1:
            print "数据总量减少30%"
            alert_item = "total_data reduced by 30%"
            print " alert_item %s" % alert_item
            if self.alert_filter(alert_client,spider_name,alert_item):
                self.weixin.push_message(token, msgtype="text", touser="@all", agentid=1000002,
                                         content=new_data["spider_name"] + u": 数据总量减少30%\n" + handler_name)
                self.dingding.push_dingding_message(url, msgtype="text", touser=mobile_num,
                                                    content=new_data["spider_name"] + " :数据总量减少30%\n" + handler_name)
        elif (not alert_flag) and flag == 2:
            print "数据总量减少50%"
            alert_item = "total_data reduced by 50%"
            print " alert_item %s" % alert_item
            if self.alert_filter(alert_client,spider_name,alert_item):
                self.weixin.push_message(token, msgtype="text", touser="@all", agentid=1000002,
                                         content=new_data["spider_name"] + u": 数据总量减少50%\n" + handler_name)
                self.dingding.push_dingding_message(url, msgtype="text", touser=mobile_num,
                                                    content=new_data["spider_name"] + " :数据总量减少50%\n" + handler_name)
        elif (not alert_flag) and flag == 3:
            print "数据总量减少70%"
            alert_item="total_data reduced by 70%"
            print " alert_item %s" % alert_item
            if self.alert_filter(alert_client,spider_name,alert_item):
               self.weixin.push_message(token, msgtype="text", touser="@all", agentid=1000002,
                                        content=new_data["spider_name"] + u": 数据总量减少70%\n" + handler_name)
               self.dingding.push_dingding_message(url, msgtype="text", touser=mobile_num,
                                                   content=new_data["spider_name"] + " :数据总量减少70%\n" + handler_name)
        else:
            # old_data[spider_name] = new_data
            alert_item = "taotal_data reduced"
            self.relive_alert(alert_client,spider_name,alert_item)
            self.conn.info.alter_info.remove({"spider_name": spider_name})
            print "remove secusseeful"
            self.conn.info.alter_info.insert(new_data)
            print "*** new_data %s" % new_data
        # 预警异常数据的比例
        abnormal_alert = self.check_data_abnormal(new_data)
        print abnormal_alert
        if (not alert_flag) and abnormal_alert == 1:
            print "%s 的异常空字符数据占比30%"
            alert_item = "empty_characters account for 30%"
            print " alert_item %s" % alert_item
            if self.alert_filter(alert_client,spider_name,alert_item):
                self.weixin.push_message(token, msgtype="text", touser="@all", agentid=1000002,
                                         content=new_data["spider_name"] + u": 空字符item占比30%\n" + handler_name)
                self.dingding.push_dingding_message(url, msgtype="text", touser=mobile_num,
                                                    content=new_data["spider_name"] + " :空字符item占比30%\n" + handler_name)
        elif (not alert_flag) and abnormal_alert == 2:
            print "%s 异常空字符数据占比50%"
            alert_item = "empty_characters account for 50%"
            print " alert_item %s" % alert_item
            if self.alert_filter(alert_client, spider_name, alert_item):
                self.weixin.push_message(token, msgtype="text", touser="@all", agentid=1000002,
                                         content=new_data["spider_name"] + u": 空字符item占比50%\n" + handler_name)
                self.dingding.push_dingding_message(url, msgtype="text", touser=mobile_num,
                                                    content=new_data["spider_name"] + " :空字符item占比50%\n" + handler_name)
        elif (not alert_flag) and abnormal_alert == 3:
            print "%s 异常空字符数据占比70%"
            alert_item = "empty_characters account for 70%"
            print " alert_item %s" % alert_item
            if self.alert_filter(alert_client, spider_name, alert_item):
                self.weixin.push_message(token, msgtype="text", touser="@all", agentid=1000002,
                                         content=new_data["spider_name"] + u": 空字符item占比70%\n" + handler_name)
                self.dingding.push_dingding_message(url, msgtype="text", touser=mobile_num,
                                                    content=new_data["spider_name"] + " :空字符item占比70%\n" + handler_name)
        else:
            alert_item = "empty_charaters account"
            self.relive_alert(alert_client, spider_name, alert_item)
        # 预警异常请求的比例
        request_abnormal_dict = self.check_request_abnormal(new_data)
        print type(new_data["spider_name"])
        if request_abnormal_dict:
            print request_abnormal_dict
            for key,value in request_abnormal_dict.iteritems():
                print "key 类型 " 
                print type(key)
                if value > 0.3:
                    if value < 0.5:
                        print "%s 的 %s 异常占比 40%"
                        if not alert_flag:
                            alert_item = str(key) + " account for 40%"
                            print " alert_item %s" % alert_item
                            if self.alert_filter(alert_client, spider_name, alert_item):
                                self.weixin.push_message(token, msgtype="text", touser="@all", agentid=1000002,
                                                         content=new_data["spider_name"] + u":" + key + u":异常占比40%\n" + handler_name)
                                self.dingding.push_dingding_message(url, msgtype="text", touser=mobile_num,
                                                                    content=new_data["spider_name"] + ":"  + key + " :异常占比40%\n" + handler_name)

                    else:
                        if value < 0.7:
                            print "%s 的 %s 异常占比 60%"
                            if not alert_flag:
                                alert_item = str(key) + " account for 60%"
                                print " alert_item %s" % alert_item
                                if self.alert_filter(alert_client, spider_name, alert_item):
                                    self.weixin.push_message(token, msgtype="text", touser="@all", agentid=1000002,
                                                             content=new_data["spider_name"] + u":" + key + u":异常占比60%\n" + handler_name)
                                    self.dingding.push_dingding_message(url, msgtype="text", touser=mobile_num,
                                                                        content=new_data["spider_name"] + ":" + key + " :异常占比60%\n" + handler_name)
                        else:
                            print "%s 的 %s 异常占比 80%"
                            if not alert_flag:
                                alert_item = str(key) + " account for 80%"
                                print " alert_item %s" % alert_item
                                if self.alert_filter(alert_client, spider_name, alert_item):
                                    self.weixin.push_message(token, msgtype="text", touser="@all",agentid=1000002,
                                                             content=new_data["spider_name"] + u":" + key + u":异常占比80%\n" + handler_name)
                                    self.dingding.push_dingding_message(url, msgtype="text", touser=mobile_num,
                                                                        content=new_data["spider_name"] + ":" + key + " :异常占比80%\n" + handler_name)
                else:
                    alert_item = str(key) + " account"
                    print " 解除预警"
                    print alert_item
                    self.relive_alert(alert_client, spider_name, alert_item)

    def run(self):
        while True:
            qsize = self.task_queue.qsize()
            print "redis size %s" % qsize
            if qsize:
                for i in range(qsize):
                    try:
                        spider_info = self.task_queue.get()
                        spider_info = json.loads(spider_info.strip())
                        print "spider_info %s" % spider_info
                        # 将存放到内存中的数据存放数据库中
                        client = self.conn["info"]
                        cursor = client.alter_info.find({"spider_name": spider_info["spider_name"]})
                        if cursor.count() == 0:
                            client.alter_info.insert(spider_info)
                        cursor = client.alter_info.find({"spider_name": spider_info["spider_name"]})
                        for alter_data in cursor:
                            print " mongo alter_data %s" % alter_data
                            self.check_data(alter_data, spider_info, spider_info["spider_name"])
                            # 检查项目的新旧数据的爬取数据的比例情况
                            # if spider_info["spider_name"] not in self.spider_info_dict:
                            # self.spider_info_dict[spider_info["spider_name"]] = spider_info.copy()
                        # self.check_data(self.spider_info_dict, spider_info, spider_info["spider_name"])
                        # print spider_info, self.spider_info_dict[spider_info["spider_name"]]
                        # db = self.conn["info"]["spiderinfocollecthistory"]
                        spider_info["start_time"] = datetime.datetime.strptime(spider_info["start_time"],
                                                                           '%Y-%m-%d %H:%M:%S')
                        spider_info["finish_time"] = datetime.datetime.strptime(spider_info["finish_time"],
                                                                            '%Y-%m-%d %H:%M:%S')

                        client.spiderinfocollecthistory.insert(spider_info)
                    except:
                        continue
            time.sleep(15)

if __name__ == "__main__":
    temp = MonitorMessage()
    temp.run()
