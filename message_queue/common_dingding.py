#-*-coding:utf-8-*-
# import urllib
import urllib2
import json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class DingdingActiveApi(object):

    def __init__(self):
        self.header_type = {'Content-Type': 'application/json', 'charset': 'utf-8'}

    def push_dingding_message(self,url,msgtype,content,touser):
        data = {
            "msgtype": msgtype,
            "text": {
                "content": content
            },
            "at": {
                "atMobiles": [
                    touser
                ],
                "isAtAll": "false"
            }
        }
        print data
        jdata = json.dumps(data, ensure_ascii=False).encode("utf-8")
        print jdata
        request = urllib2.Request(url=url,headers=self.header_type,data=jdata)
        response = json.loads(urllib2.urlopen(request).read())
        return response['errcode'], response['errmsg'],response

