from __future__ import print_function, division, absolute_import

import logging
import os
# import socket

import aiohttp
from aiohttp import web

from .. import metrics
# from ..utils import log_errors
# from ..comm.utils import get_tcp_server_address


logger = logging.getLogger(__name__)


# class RequestHandler(web.RequestHandler):
#     def initialize(self, server=None):
#         logger.debug("Connection %s", self.request.uri)
#         if server:
#             self.server = server


def resource_collect(pid=None):
    """Gather system usage stats.

    Returns empty dict `{}` if psutil is not installed.

    Parameters
    ----------
    pid : None (default) or int
        process to check - this one if None
    """
    try:
        import psutil
    except ImportError:
        return {}

    p = psutil.Process(pid or os.getpid())
    return {'cpu_percent': psutil.cpu_percent(),
            'status': p.status(),
            'memory_percent': p.memory_percent(),
            'memory_info': p.memory_info(),
            'disk_io_counters': metrics.disk_io_counters(),
            'net_io_counters': metrics.net_io_counters()}


def resource_handler(request):
    return web.json_response(resource_collect())

# class Resources(RequestHandler):
#     """Served details about this process and machine"""
#     def get(self):
#         self.write(resource_collect())

async def proxy_handler(request):
    ip = request.match_info['ip']
    port = request.match_info['port']
    rest = request.match_info['rest']
    url = "http://{}:{}/%s".format(ip, port, rest)
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return web.json_response(await response.json())


# class Proxy(RequestHandler):
#     """Send REST call to specific worker return its response"""
#     @gen.coroutine
#     def get(self, ip, port, rest):
#         client = AsyncHTTPClient()
#         response = yield client.fetch("http://%s:%s/%s" % (ip, port, rest))
#         self.write(response.body)  # TODO: capture more data of response


# class MyApp(HTTPServer):
#     _port = None
#
#     @property
#     def port(self):
#         if self._port is None:
#             try:
#                 self._port = get_tcp_server_address(self)[1]
#             except RuntimeError:
#                 raise OSError("Server has no port.  Please call .listen first")
#         return self._port
#
#     def listen(self, addr):
#         if isinstance(addr, tuple):
#             ip, port = addr
#         else:
#             port = addr
#             ip = None
#         while True:
#             try:
#                 super(MyApp, self).listen(port, ip)
#                 break
#             except (socket.error, OSError) as e:
#                 if port:
#                     raise
#                 else:
#                     logger.info('Randomly assigned port taken for %s. Retrying',
#                                 type(self).__name__)

class MyApp(web.Application):

    @property
    def port(self):
        return self['port']
