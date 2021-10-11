from guppy import hpy
from celery.signals import task_prerun, task_postrun
from django.db import reset_queries
from djperf.utils import print_slow_queries


heap = hpy()


def convert_size(size):
    if size <1024:
        return size
    elif (size >= 1024) and (size < (1024 * 1024)):
        return "%.2f KB"%(size/1024)
    elif (size >= (1024*1024)) and (size < (1024*1024*1024)):
        return "%.2f MB"%(size/(1024*1024))
    else:
        return "%.2f GB"%(size/(1024*1024*1024))


@task_prerun.connect()
def task_prerun(**kwargs):
    reset_queries()
    hp = heap.heap()
    print(kwargs)
    print("MEM BEFORE: ", convert_size(hp.size))


@task_postrun.connect()
def task_postrun(**kwargs):
    hp = heap.heap()
    print_slow_queries(kwargs.get('task').name)
    print("MEM AFTER: ", convert_size(hp.size))

