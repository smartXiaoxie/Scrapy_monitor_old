# coding=utf-8
import urllib
import urllib2
import json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


class WeiXinActiveApi(object):

    def __init__(self):
        self.header = {'Content-Type': 'application/x-www-form-urlencoded', 'charset': 'utf-8'}

    def get_token(self, corpid, corpsecret):
        url = "https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid=%s&corpsecret=%s" % (corpid, corpsecret)
        request = urllib2.Request(url=url, headers=self.header)
        response = json.loads(urllib2.urlopen(request).read())
        if "access_token" in response:
            return 0, response['access_token']
        else:
            return -1, response['errcode']

    def push_message(self, token, msgtype, agentid, content, touser):
        data = {
            "touser": touser,
            "msgtype": msgtype,
            "agentid": agentid,
            "text": {
                "content": content
            },
            "safe": 0
        }
        url = "https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token=%s" % token
        jdata = json.dumps(data, ensure_ascii=False).encode("utf-8")
        request = urllib2.Request(url=url, headers=self.header, data=jdata)
        response = json.loads(urllib2.urlopen(request).read())
        print  response
        return response['errcode'], response['errmsg']

    def get_exists_token(self, account):
        action = "get_token"
        url = "http://10.10.92.98:8081/weixin"
        # url = "http://127.0.0.1:8000/weixin"
        data = {
            "action": action,
            "account": account
        }
        jdata = urllib.urlencode(data)
        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor())

        response = opener.open(urllib2.Request(url), jdata).read()
        return response

