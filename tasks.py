"""
TODO: Implement performance tracking for all tasks at once.
Execution time, Query time, Query count, Memory Usage
"""
import logging
from celery.signals import task_prerun, task_postrun
from django.db import reset_queries
from .utils import print_slow_queries, convert_size, install
install('guppy3')
from guppy import hpy


heap = hpy()


warn = logging.getLogger().warning


@task_prerun.connect()
def task_prerun(**kwargs):
    reset_queries()
    hp = heap.heap()
    print(kwargs)
    warn("MEM BEFORE: %s", convert_size(hp.size))


@task_postrun.connect()
def task_postrun(**kwargs):
    hp = heap.heap()
    print_slow_queries(kwargs.get('task').name)
    warn("MEM AFTER: %s", convert_size(hp.size))
