# coding=utf-8
"""Common middleware"""
import logging

import time

from django.db import connection


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

    def process_request(self, request):
        request.start_time = time.time()

    def process_response(self, request, response):
        """Process request"""
        logger = logging.getLogger()
        response_time = time.time() - request.start_time
        if response_time < 0.4:
            return response
        times = []
        slow_queries = []
        for q in connection.queries:
            time_ = float(q['time'])
            if time_ > 0.1:
                slow_queries.append(q)
            times.append(time_)
        logger.warning(request.path + str(request.GET.urlencode()))
        logger.warning("Total time: %s", response_time)
        logger.warning("Queries Total time: %s", sum(times))
        logger.warning("Queries: %s", len(times))
        for q in sorted(slow_queries, key=lambda x: x['time']):
            logger.warning("Slow query: sql: %s time: %s", q['sql'], q['time'])
        return response
