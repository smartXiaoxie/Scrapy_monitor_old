# coding=utf-8
from twisted.internet import task
from scrapy import signals
import urllib2
import urllib
import time
from scrapy import __version__

# 这个模块是给爬虫程序做监控爬虫状态的模块，里面结合了scrapy 模块使用。
class SpiderWatcher(object):

    def __init__(self, stats, interval=30.0):
        self.stats = stats
        self.interval = interval
        self.multiplier = 60.0 / self.interval
        self.collection_stats_task = None
        self.avg_rate_task = None
        self.item_empty_rate = 0.2
        self.spider_items_len = {}
        self.spider_count = {}
        self.items_avg_count = {}
        self.interval_avg = 20
        self.request_dropped_count = {}

    def send_message_to_monitor(self, message, action):
        message['action'] = action
        data = urllib.urlencode(message)
        # req = urllib2.Request('http://106.75.12.195:8080/spider/watcher', data)
        # req = urllib2.Request('http://127.0.0.1:8000/spider/watcher', data)
        req = urllib2.Request('http://10.10.92.98:8080/spider/watcher', data)
        urllib2.urlopen(req)

    # sibnals 是scrapy 的信号绑定的机制，当scrapy 给绑定的函数执行也会执行绑定的函数，但是执行的东西是不一样的，也互不干扰。
    @classmethod
    def from_crawler(cls, crawler):
        interval = crawler.settings.getfloat('LOGSTATS_INTERVAL')

        ext = cls(crawler.stats, interval)
        ext.statsClt = crawler.stats
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)
        return ext

    def spider_opened(self, spider):
        self.spider_count[spider.name] = {'pagesprev': 0,
                                          'itemsprev': 0}

        self.items_avg_count[spider.name] = {'items': 0}
        self.item_abnormal_count = {spider.name: 0}
        self.start_time = time.time()

        self.crawl_init(spider)
        self.collection_stats_task = task.LoopingCall(self.crawl_rate, spider)
        self.avg_rate_task = task.LoopingCall(self.crawl_rate_avg, spider)

        self.avg_rate_task.start(self.interval_avg)
        self.collection_stats_task.start(self.interval)

    def spider_closed(self, spider):
        spider_stats = self.statsClt.get_stats()
        self.finish_time = time.time()

        # 别忘记关定时任务
        if self.collection_stats_task and self.collection_stats_task.running:
            self.collection_stats_task.stop()

        if self.avg_rate_task and self.avg_rate_task.running:
            self.avg_rate_task.stop()

        spider_stats.update({"download/request_dropped": self.request_dropped_count[spider.name]})

        msg = {"spider_name": spider.name,
               "finish_time": self.finish_time,
               "request_stats": spider_stats,
               "data_stats": {"abnormal": self.item_abnormal_count[spider.name],
                              "total": self.stats.get_value('item_scraped_count', 0)},
               "spider_rate": 0}
        self.send_message_to_monitor(msg, "spider_closed")

    def crawl_init(self, spider):
        spider_stats = self.statsClt.get_stats()

        if spider.name not in self.request_dropped_count:
            self.request_dropped_count[spider.name] = 0

        spider_stats.update({"download/request_dropped": self.request_dropped_count[spider.name]})
        msg = {
            "spider_name": spider.name,
            "request_stats": spider_stats,
            "start_time": self.start_time,
            "data_stats": {"abnormal": self.item_abnormal_count[spider.name], "total": 0},
            "spider_rate": 0,
            "spider_version": __version__
        }
        self.send_message_to_monitor(msg, "spider_open")

    def crawl_rate_avg(self, spider):
        items = self.items_avg_count[spider.name]['items']
        self.items_avg_count[spider.name]['items'] = 0
        msg = {
            "spider_name": spider.name,
            "spider_avg_rate": items
        }
        self.send_message_to_monitor(msg, "spider_avg_rate")

    def crawl_rate(self, spider):
        items = self.stats.get_value('item_scraped_count', 0)
        irate = (items - self.spider_count[spider.name]['itemsprev']) * self.multiplier
        self.spider_count[spider.name]['itemsprev'] = items
        spider_stats = self.statsClt.get_stats()
        spider_stats.update({"download/request_dropped": self.request_dropped_count[spider.name]})
        msg = {"spider_name": spider.name,
               "request_stats": spider_stats,
               "data_stats": {"abnormal": self.item_abnormal_count[spider.name], "total": items},
               "spider_rate": irate
               }
        self.send_message_to_monitor(msg, "spider_stats")

    def item_scraped(self, item, spider):

        if item:
            item_name = item.__class__.__name__
            if spider.name not in self.spider_items_len:
                self.spider_items_len[spider.name] = {}
            if item_name not in self.spider_items_len[spider.name]:
                self.spider_items_len[spider.name][item_name] = len(item)

            item_empty_count = 0
            item_count = 0
            for attr in item:
                if not item[attr]:
                    item_empty_count += 1
                item_count += 1

            if item_empty_count > self.spider_items_len[spider.name][item_name] * self.item_empty_rate:
                self.item_abnormal_count[spider.name] += 1
            elif item_count > self.spider_items_len[spider.name][item_name]:
                self.item_abnormal_count[spider.name] += 1

            self.items_avg_count[spider.name]['items'] += 1

    def request_dropped(self, spider):
        if spider.name not in self.request_dropped_count:
            self.request_dropped_count[spider.name] = 1
        else:
            self.request_dropped_count[spider.name] += 1











