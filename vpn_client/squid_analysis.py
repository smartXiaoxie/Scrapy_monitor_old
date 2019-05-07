# coding=utf-8
import time
import re
import json
import urllib2
import urllib
import datetime
import os
import sys

def exe_cmd(command, timeout=180):
    import subprocess, datetime, os, time, signal
    # cmd = command.split(" ")
    start = datetime.datetime.now()
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    while process.poll() is None:
        time.sleep(0.2)
        now = datetime.datetime.now()
        if (now - start).seconds > timeout:
            os.kill(process.pid, signal.SIGKILL)
            os.waitpid(-1, os.WNOHANG)
            return None
    return process.stdout.read()


def get_ip():
    ip = exe_cmd("/sbin/ifconfig eth0 | grep 'inet addr' | awk -F: '{print $2}' | awk '{print $1}'")
    return ip.split('\n')[0]


def create_log(dst_file_name):
    exe_cmd("tail -10000 /var/log/squid/access.log > %s" % dst_file_name)

def get_proxy_ip():
    ip = exe_cmd("curl icanhazip.com")
    return ip.strip()

class Alarm(object):

    def send_message_to_monitor(self, message, action):
        message['action'] = action
        data = urllib.urlencode(message)
        try:
            # req = urllib2.Request('http://127.0.0.1:8081/vpn_info', data)
            req = urllib2.Request('http://10.10.92.98:8081/vpn_info', data)
            urllib2.urlopen(req)
        except Exception as e:
            print e


class UrlRule(object):

    """
        防止有些网站就算换ip也会返回非正常http stats
    """

    def __init__(self):
        self.black_list = [
            "mogujie"
        ]

    def check(self, url_name):

        for line in self.black_list:
            if url_name.find(line) > -1:
                return False
        return True


class DataAnalysis(object):

    def __init__(self, log_path):
        self.log_path = log_path
        self.data_list = []
        self.http_stats = {}
        self.url_stats = {}
        self.url_rule = UrlRule()
        self.Alarm = Alarm()
        self.data_len = 0

    def open_log(self):
        self.data_list = open(self.log_path, 'r').readlines()
        self.data_len = len(self.data_list)

    def split_attr(self, data_str):
        pattern = re.compile(r"[ ]*")
        attr_list = re.split(pattern, data_str)   split
        return attr_list
    
    def get_account_info(self, file_path):
        fp = open(file_path, 'r')
        account_info = re.split("[ ]*", fp.readlines()[1])
        return account_info[4], account_info[6], account_info[8]

    def get_domain_name(self, url):
        pattern = re.compile(r"(.*?\.com)|(.*?\.cn)")
        domain_name = pattern.match(url)
        return domain_name.group()

    def analysis(self):
        # 计算时间差值
        start_time = self.split_attr(self.data_list[0])[0]
        start_time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(start_time)))
        start_time = datetime.datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')

        end_time = self.split_attr(self.data_list[-1])[0]
        end_time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(end_time)))
        end_time = datetime.datetime.strptime(end_time_str, '%Y-%m-%d %H:%M:%S')

        request_speed = (self.data_len) / (end_time - start_time).seconds

        for line in self.data_list:
            request = self.split_attr(line.strip())
            if self.url_rule.check(request[6]):
                stats_name, stats = request[3].split('/')
                if stats not in self.http_stats:
                    self.http_stats[stats] = 0
                self.http_stats[stats] += 1

                domain_name = self.get_domain_name(request[6])
                domain_name = domain_name.replace('.', '-')
                if domain_name not in self.url_stats:
                    self.url_stats[domain_name] = {}

                if stats not in self.url_stats[domain_name]:
                    self.url_stats[domain_name][stats] = 0
                self.url_stats[domain_name][stats] += 1
            else:
                self.data_len -= 1
        
        stats_count = 0
        for url in self.url_stats:
            if "200" in self.url_stats[url]:
                stats_count += self.url_stats[url]['200']
            elif "000" in self.url_stats[url]:
                stats_count += self.url_stats[url]['000']
        if (self.data_len - stats_count) / (self.data_len * 1.0) > 0.5:
            exe_cmd("python vpn_util.py")
             

        return self.http_stats, self.url_stats, request_speed

    def send_message(self):
        self.open_log()
        http_stats, url_stats, request_speed = self.analysis()
        http_count = {}
        for url in url_stats:
            if url not in http_count:
                http_count[url] = {}
                http_count[url]["count"] = 0

            http_count[url]["stats_num"] = len(url_stats[url])
            http_count[url]["count"] = reduce(lambda x, y: x+y, url_stats[url].values())
        domain, account, passwords = self.get_account_info('/root/bin/vpn.sh')
        data = {
            'ip': get_ip(),
            'stats': json.dumps(http_stats),
            'http_stats': json.dumps(url_stats),
            'http_count': json.dumps(http_count),
            'request_speed': request_speed,
            'domain': domain,
            'account': account,
            'passwords': passwords,
            'proxy_ip': get_proxy_ip()
        }
        self.Alarm.send_message_to_monitor(data, "vpninfo")

if __name__ == "__main__":
    create_log("/root/vpn/vpn_util/analysis_data.log")
    temp = DataAnalysis("/root/vpn/vpn_util/analysis_data.log")
    temp.send_message()

