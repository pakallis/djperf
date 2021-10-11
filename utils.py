from collections import Counter
import cProfile
from functools import wraps
import logging
import pstats
import re
import time

from django.db import connection, reset_queries

logger = logging.getLogger()


# TODO: Detect if celery task
# TODO: TASK_EAGER_LOAD
# TODO: Do not remove task decorator when profiling
# TODO: Detect N+1 queries
# TODO: Auto register tasks for profiling

def install(package):
    import importlib
    try:
        importlib.import_module(package)
    except (ImportError, ModuleNotFoundError):
        import pip
        pip.main(['install', package])


SLOW_QUERY_THRESHOLD = 0.02  # TODO: Move in settings


def convert_size(size):
    if size < 1024:
        return size
    elif (size >= 1024) and (size < (1024 * 1024)):
        return "%.2f KB"%(size/1024)
    elif (size >= (1024*1024)) and (size < (1024*1024*1024)):
        return "%.2f MB"%(size/(1024*1024))
    else:
        return "%.2f GB"%(size/(1024*1024*1024))


def nplusone(fn, ignore=None):
    install('nplusone')
    from nplusone.core import profiler
    import nplusone.ext.django
    if ignore is None:
        whitelist = []
    else:
        whitelist = [
            {'label': 'unused_eager_load', 'model': '*'}
        ]
        for ig in ignore:
            whitelist += {'label': 'n_plus_one', 'model': ig}

    def decorated_fn(*args, **kwargs):
        with profiler.Profiler(whitelist):
            return fn(*args, **kwargs)
    return decorated_fn


def mprof():
    install('memory_profiler')
    install('guppy3')

    has_guppy = True
    try:
        from guppy import hpy
    except ImportError:
        has_guppy = False
        print("guppy not found")

    if has_guppy:
        heap = hpy()

        def decorator(fn):
            def inner(*args, **kwargs):
                heap_before = heap.heap()
                logger.warning("Total Heap Size before: %s", convert_size(heap_before.size))
                result = fn(*args, **kwargs)
                heap_after = heap.heap()
                logger.warning("Total Heap Size after: %s", convert_size(heap_after.size))
                return result
            return inner
    else:
        def decorator(fn):
            def inner(*args, **kwargs):
                return fn(*args, **kwargs)
            return inner
    return decorator


def pprof(sort_args=None, print_args=None, times=1):
    if sort_args is None:
        sort_args = ['cumulative']
    if print_args is None:
        print_args = [20]
    def decorator(fn):
        @wraps(fn)
        def inner(*args, **kwargs):
            result = None
            profiler = cProfile.Profile()
            try:
                profiler.enable()
                for i in range(times):
                    result = fn(*args, **kwargs)
            finally:
                profiler.disable()
                stats = pstats.Stats(profiler)
                stats.sort_stats(*sort_args).print_stats(*print_args)
                stats.dump_stats('stats.prof')
            return result
        return inner
    return decorator


def lprof(times=1):
    """
    TODO: Find if decorated with task and get original function.
    Also, use TASK_ALWAYS_EAGER
    """
    install('line_profiler')
    from line_profiler import LineProfiler
    def decorator(fn):
        profiler = LineProfiler()
        profiler.add_function(fn)
        def inner(*args, **kwargs):
            result = None
            try:
                profiler.enable_by_count()
                for _ in range(times):
                    result = fn(*args, **kwargs)
            finally:
                profiler.disable_by_count()
                profiler.print_stats()
                profiler.dump_stats('stats')
            return result
        return inner
    return decorator


def slow():
    def decorator(fn):
        def inner(*args, **kwargs):
            reset_queries()
            start = time.time()
            res = fn(*args, **kwargs)
            print("fn time: ", time.time() - start)
            print_slow_queries(fn.__name__)
            return res
        return inner
    return decorator


def print_slow_queries(name):
    install('tabulate')
    logger = logging.getLogger()
    times = []
    slow_queries = []
    queries_sql = []
    for q in connection.queries:
        time_ = float(q['time'])
        if time_ > SLOW_QUERY_THRESHOLD:
            slow_queries.append(q)
        times.append(time_)
        queries_sql.append(q['sql'])

    logger.warning(name)

    for q in sorted(slow_queries, key=lambda x: x['time']):
        logger.warning("Slow query: sql: %s time: %s", q['sql'], q['time'])

    logger.warning("-" * 100)

    print_slowest_queries(connection.queries)
    print_counts_per_table(queries_sql)
    print_times_per_table(connection.queries)
    log(f"{len(times)} Queries - Total time: {round(sum(times), 3)} (sec)")


def print_slowest_queries(queries, top=5):
    import re
    logger.warning(
        "TOP %s SLOWEST QUERIES: %s", top,
        "\n".join(
            map(
                lambda x: re.sub(r'SELECT (.*)? FROM', 'SELECT ... FROM', x['sql'] + f" - {x['time']} (sec)"),
                list(sorted(queries, key=lambda x: float(x['time']), reverse=True))[:top]
            )
        )
    )
    logger.warning("-" * 100)


def print_counts_per_table(queries_sql):
    from tabulate import tabulate
    tables = []
    for q in queries_sql:
        table = re.search(r'FROM "(.*?)"', q)
        if table is not None:
            tables.append(table.groups()[0])
        else:
            tables.append('OTHER')
    log(tabulate(Counter(tables).most_common(10), headers=['Table', 'Query Count']))


def log(msg):
    import sys
    sys.stdout.write('\n')
    sys.stdout.write(msg)
    sys.stdout.write('\n')
    sys.stdout.flush()


def print_times_per_table(queries):
    # Print also total times for each query group
    from collections import defaultdict
    from tabulate import tabulate

    times_per_table = defaultdict(lambda: 0)
    for q in queries:
        sql = q['sql']
        table = re.search(r'FROM "(.*?)"', sql)
        if table is not None:
            times_per_table[table.groups()[0]] += float(q['time'])
        else:
            times_per_table['OTHER'] += float(q['time'])

    sorted_times_per_table = [(k, v) for k,v in sorted(times_per_table.items(), key=lambda x: x[1], reverse=True)][:10]
    log(tabulate(sorted_times_per_table, headers=['Table', 'Total Time (sec)']))


def count_queries(fn):
    """TODO: Count queries on multiple invocations of function"""
    from django.db import connection, reset_queries
    logger = logging.getLogger()
    def decorated_fn(*args, **kwargs):
        reset_queries()
        res = fn(*args, **kwargs)
        logger.warning((connection.queries))
        return res
    return decorated_fn
