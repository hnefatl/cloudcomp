import scheduler
from kubernetes import client, config

APPS = [scheduler.App("spark", "driver"), scheduler.App("custom", "")]

config.load_kube_config()
kube = client.CoreV1Api()
sc = scheduler.Scheduler(kube, APPS)

if __name__ == "__main__":
    n = len(sc.deallocated())
    sc.allocate(APPS[0], n // 2)
    s = input()
    sc.allocate(APPS[1], n // 2)
    s = input()
    sc.transfer(APPS[0], APPS[1])
    s = input()
    sc.transfer(APPS[1], APPS[0], 2)

    print(len(sc.deallocated()))
