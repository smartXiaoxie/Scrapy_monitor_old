import os
import json
import urllib
import urllib2
from get_sys_info import WatcherSysUtil


def send_message_to_monitor(message, action):
    message['action'] = action
    data = urllib.urlencode(message)
    try:
        # req = urllib2.Request('http://127.0.0.1:8081/cluster', data)
        req = urllib2.Request('http://10.10.92.98:8081/cluster', data)
        urllib2.urlopen(req)
    except Exception as e:
        print e

if __name__ == "__main__":

    num = 0
    now_num = 0
    cpu_sum = 0.0
    memory_sum = 0.0

    sys_info = WatcherSysUtil()
    cpu_percent = float(sys_info.get_cpu_percent())
    memory_percent = float(sys_info.get_memory_percent())
    ip_name = sys_info.get_ip()
    # ip_name = "127.0.0.1"

    if not os.path.exists('configure.txt'):
        configure = open('configure.txt', 'w')
        configure.write('30,1,%s,%s' % (str(cpu_percent), str(memory_percent)))
        num = 30
        configure.close()

    else:
        fp = open('configure.txt', 'r+')
        num, now_num, cpu_sum, memory_sum = fp.readlines()[0].strip().split(',')
        fp.seek(0, 0)
        num = int(num)
        if int(now_num) == int(num):
            fp.write('%s,%s,%s,%s                ' %
                     (str(num), str(1),
                      str(cpu_percent),
                      str(memory_percent)))
        else:
            fp.write('%s,%s,%s,%s                ' %
                     (str(num), str((int(now_num)+1)), str(float(cpu_sum)+cpu_percent),
                      str(float(memory_sum)+memory_percent)))
        fp.close()

    cpu_kernel = sys_info.get_cpu_kernel()
    memory_capacity = float(sys_info.get_memeory_capacity())
    disk = sys_info.get_disk_partition()
    sys_disk = float(disk[0][1])
    data_disk = float(disk[1][1])
    net_io = float(sys_info.get_net_io())

    if int(now_num) == int(num):
        data = {
                'cpu': cpu_percent,
                'memory': memory_percent,
                'sys_disk': sys_disk,
                'net_io': round(net_io, 2),
                'cpu_kernel': cpu_kernel,
                'memory_capacity': round(memory_capacity / 1048576 / 1024.0, 0),
                'cpu_avg': round((float(cpu_sum)) / int(num), 2),
                'memory_avg': round((float(memory_sum)) / int(num), 2),
                'data_disk': data_disk
                }

        send_message_to_monitor({'data': json.dumps(data), 'ip': ip_name}, "client")
    else:
        data = {
                'cpu': float(cpu_percent),
                'memory': memory_percent,
                'sys_disk': sys_disk,
                'net_io': round(net_io, 2),
                'cpu_kernel': cpu_kernel,
                'memory_capacity': round(memory_capacity / 1048576 / 1024.0, 0),
                'data_disk': data_disk
                }
        send_message_to_monitor({'data': json.dumps(data), 'ip': ip_name}, "client")



