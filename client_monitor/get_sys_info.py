import psutil
import time

class WatcherSysUtil(object):

    def __init__(self):
        pass

    def get_ip(self):
        return psutil.net_if_addrs()['eth0'][0][1]

    def get_cpu_percent(self):
        return psutil.cpu_percent(1)

    def get_memory(self):
        return int(psutil.virtual_memory().total / (1024*1024))

    def get_cpu_kernel(self):
        return psutil.cpu_count(logical=False)

    def get_memeory_capacity(self):
        return psutil.virtual_memory()[0]

    def get_memory_percent(self):
        return psutil.virtual_memory().percent

    def get_disk_partition(self):
        sys_disk = ('', -1)
        max_disk = ('', -1)
        for disk in psutil.disk_partitions():
            try:
                if disk[1] == '/':
                    sys_disk = (disk[1], psutil.disk_usage(disk[1])[-1])
                elif max_disk[1] < psutil.disk_usage(disk[1])[-1]:
                    max_disk = (disk[1], psutil.disk_usage(disk[1])[-1])
            except Exception as e:
                pass
        return sys_disk, max_disk

    def get_net_io(self):
        byte_sum = 0
        for i in range(3):
            r1 = psutil.net_io_counters().bytes_recv
            time.sleep(1)
            r2 = psutil.net_io_counters().bytes_recv
            byte_sum = r2 - r1
        return byte_sum / 10240.0

if __name__ == "__main__":
    temp = WatcherSysUtil()
    print temp.get_cpu_percent()
    print temp.get_memory()
    print temp.get_memory_percent()
    print temp.get_disk_partition()
    print temp.get_net_io()





