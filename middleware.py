# coding=utf-8
"""Common middleware"""
import logging
import time
from djperf.utils import print_slow_queries


def convert_size(size):
    if size <1024:
        return size
    elif (size >= 1024) and (size < (1024 * 1024)):
        return "%.2f KB"%(size/1024)
    elif (size >= (1024*1024)) and (size < (1024*1024*1024)):
        return "%.2f MB"%(size/(1024*1024))
    else:
        return "%.2f GB"%(size/(1024*1024*1024))


class MiddlewareMixin:
    def __init__(self, get_response=None):
        self.get_response = get_response

    def __call__(self, request):
        response = None
        if hasattr(self, 'process_request'):
            response = self.process_request(request)
        response = response or self.get_response(request)
        if hasattr(self, 'process_response'):
            response = self.process_response(request, response)
        return response


class SQLLog(MiddlewareMixin):
    logger = logging.getLogger()
    slow_response_time = 0.001

    def process_request(self, request):
        request.start_time = time.time()

    def process_response(self, request, response):
        """Process request"""
        from .utils import log

        response_time = time.time() - request.start_time
        if response_time < self.slow_response_time:
            return response
        print_slow_queries(str(request.path + str(request.GET.urlencode())))
        log(f"Response Time: {round(response_time, 3)} (sec)")
        return response

