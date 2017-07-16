from __future__ import print_function, division, absolute_import

import asyncio
from collections import defaultdict
import json
import logging

import aiohttp
from aiohttp import web

from .core import MyApp, resource_handler, proxy_handler
from ..utils import key_split, log_errors
from ..compatibility import unicode


logger = logging.getLogger(__name__)


def ensure_string(s):
    if not isinstance(s, unicode):
        s = s.decode()
    return s


def info_handler(request):
    """ Basic info about the scheduler """
    server = request.app['server']
    return web.json_response({'ncores': server.ncores,
                              'status': server.status})

# class Info(RequestHandler):
#     """ Basic info about the scheduler """
#     def get(self):
#         self.write({'ncores': self.server.ncores,
#                     'status': self.server.status})


def processing_handler(request):
    """ Active tasks on each worker."""
    server = request.app['server']
    resp = {addr: [ensure_string(key_split(t)) for t in tasks]
            for addr, tasks in server.processing.items()}
    return web.json_response(resp)

# class Processing(RequestHandler):
#     """ Active tasks on each worker """
#     def get(self):
#         resp = {addr: [ensure_string(key_split(t)) for t in tasks]
#                 for addr, tasks in self.server.processing.items()}
#         self.write(resp)


async def broadcast_handler(request):
    """
    Broadcast request to all workers with an HTTP servoce enabled,
    collate their responses.
    """
    rest = request.match_info['rest']
    server = request.app['server']
    addresses = [(addr, d['host'], d['services']['http'])
                 for addr, d in server.worker_info.items()
                 if 'http' in d['services']]

    async def fetch(sess, addr, url):
        async with sess.get(url) as resp:
            return addr, await resp.json()

    requests = []
    async with aiohttp.ClientSession() as session:
        for addr, host, port in addresses:
            url = "http://{}:{}/{}".format(host, port, rest)
            requests.append(fetch(session, addr, url))

        data = {k: v for k, v in await asyncio.gather(*requests)}

    return web.json_response(data)


# class Broadcast(RequestHandler):
#     """
#     Broadcast request to all workers with an HTTP servoce enabled,
#     collate their responses.
#     """
#     @gen.coroutine
#     def get(self, rest):
#         """
#         Broadcast request.  *rest* is the path in the HTTP hosts'
#         URL space (e.g. "/foo/bar/somequery?id=5").
#         """
#         addresses = [(addr, (d['host'], d['services']['http']))
#                      for addr, d in self.server.worker_info.items()
#                      if 'http' in d['services']]
#         client = AsyncHTTPClient()
#         responses = {addr: client.fetch("http://%s:%s/%s" %
#                                         (http_host, http_port, rest))
#                      for addr, (http_host, http_port) in addresses}
#         responses2 = yield responses
#         responses3 = {k: json.loads(v.body.decode())
#                       for k, v in responses2.items()}
#         self.write(responses3)  # TODO: capture more data of response


def memory_load_handler(request):
    """The total amount of data held in memory by workers"""
    server = request.app['server']
    data = {worker: sum(server.nbytes[k] for k in keys)
            for worker, keys in server.has_what.items()}
    return web.json_response(data)


# class MemoryLoad(RequestHandler):
#     """The total amount of data held in memory by workers"""
#     def get(self):
#         self.write({worker: sum(self.server.nbytes[k] for k in keys)
#                    for worker, keys in self.server.has_what.items()})

def memory_load_by_key_handler(request):
    """The total amount of data held in memory by workers"""
    server = request.app['server']
    out = {}
    for worker, keys in server.has_what.items():
        d = defaultdict(lambda: 0)
        for key in keys:
            d[key_split(key)] += server.nbytes[key]
        out[worker] = {k: v for k, v in d.items()}
    return web.json_response(out)


# class MemoryLoadByKey(RequestHandler):
#     """The total amount of data held in memory by workers"""
#     def get(self):
#         out = {}
#         for worker, keys in self.server.has_what.items():
#             d = defaultdict(lambda: 0)
#             for key in keys:
#                 d[key_split(key)] += self.server.nbytes[key]
#             out[worker] = {k: v for k, v in d.items()}
#         self.write(out)

def tasks_handler(request):
    """ Lots of information about all the workers """
    with log_errors():
        from ..diagnostics.scheduler import tasks
        return web.json_response(tasks(request.app['server']))


# class Tasks(RequestHandler):
#     """ Lots of information about all the workers """
#     def get(self):
#         with log_errors():
#             from ..diagnostics.scheduler import tasks
#             self.write(tasks(self.server))


def workers_handler(request):
    with log_errors():
        from ..diagnostics.scheduler import workers
        return web.json_response(workers(request.app['server']))

# class Workers(RequestHandler):
#     """ Lots of information about all the workers """
#     def get(self):
#         with log_errors():
#             from ..diagnostics.scheduler import workers
#             self.write(workers(self.server))


def scheduler_app(scheduler, **kwargs):
    app = web.Application(**kwargs)
    app['server'] = scheduler
    router = app.router
    router.add_get('/info.json', info_handler)
    router.add_get('/resources.json', resource_handler)
    router.add_get('/processing.json', processing_handler)
    router.add_get(r'/{ip:[\w.-]+}:{port:\d+}/{rest:.+}', proxy_handler)
    router.add_get(r'/broadcast/{rest:.+}', broadcast_handler)
    router.add_get(('/tasks.json', tasks_handler))
    router.add_get('/workers.json', workers_handler)
    router.add_get('/memory-load.json', memory_load_handler)
    router.add_get('/memory-load-by-key.json', memory_load_by_key_handler)
    return app

# def HTTPScheduler(scheduler, **kwargs):
#     application = MyApp(web.Application([
#         (r'/info.json', Info, {'server': scheduler}),
#         (r'/resources.json', Resources, {'server': scheduler}),
#         (r'/processing.json', Processing, {'server': scheduler}),
#         (r'/proxy/([\w.-]+):(\d+)/(.+)', Proxy),
#         (r'/broadcast/(.+)', Broadcast, {'server': scheduler}),
#         (r'/tasks.json', Tasks, {'server': scheduler}),
#         (r'/workers.json', Workers, {'server': scheduler}),
#         (r'/memory-load.json', MemoryLoad, {'server': scheduler}),
#         (r'/memory-load-by-key.json', MemoryLoadByKey, {'server': scheduler}),
#         ]), **kwargs)
#     return application
