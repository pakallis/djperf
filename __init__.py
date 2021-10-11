from contextlib import contextmanager

from django.db import reset_queries, connection

from .utils import lprof, slow, print_slow_queries, pprof, mprof, nplusone
from .middleware import SQLLog

@contextmanager
def c_queries():
    reset_queries()
    yield
    for q in connection.queries:
        print(q)
    print(len(connection.queries))
