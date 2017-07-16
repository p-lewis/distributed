from __future__ import print_function, division, absolute_import

from collections import defaultdict
import logging
import os

from aiohttp import web
from toolz import keymap, valmap, pluck

from .core import MyApp, resource_handler
from ..sizeof import sizeof
from ..utils import key_split


logger = logging.getLogger(__name__)


def info_handler(request):
    """Basic info about the worker."""
    server = request.app['server']
    resp = {'ncores': server.ncores,
            'nkeys': len(server.data),
            'status': server.status}
    return web.json_response(resp)


# class Info(RequestHandler):
#     """Basic info about the worker """
#     def get(self):
#         resp = {'ncores': self.server.ncores,
#                 'nkeys': len(self.server.data),
#                 'status': self.server.status}
#         self.write(resp)

def processing_handler(request):
    server = request.app['server']
    resp = {'processing': list(map(str, server.executing)),
            'waiting': list(map(str, server.waiting_for_data)),
            'constrained': list(map(str, server.constrained)),
            'ready': list(map(str, pluck(1, server.ready)))}


# class Processing(RequestHandler):
#     def get(self):
#             resp = {'processing': list(map(str, self.server.executing)),
#                     'waiting': list(map(str, self.server.waiting_for_data)),
#                     'constrained': list(map(str, self.server.constrained)),
#                     'ready': list(map(str, pluck(1, self.server.ready)))}
#             self.write(resp)


def nbytes_handler(request):
    resp = request.app['server'].nbytes
    return web.json_response(resp)


# class NBytes(RequestHandler):
#     """Basic info about the worker """
#     def get(self):
#         resp = self.server.nbytes
#         self.write(resp)


def nbytes_summary(request):
    out = defaultdict(lambda: 0)
    server = request.app['server']
    for k in server.data:
        out[key_split(k)] += server.nbytes[k]
    return web.json_response(out)


# class NBytesSummary(RequestHandler):
#     """Basic info about the worker """
#     def get(self):
#         out = defaultdict(lambda: 0)
#         for k in self.server.data:
#             out[key_split(k)] += self.server.nbytes[k]
#         self.write(dict(out))


def local_files_handler(request):
    files = os.listdir(request.app['server'].local_dir)
    return web.json_response({'files': files})


# class LocalFiles(RequestHandler):
#     """List the local spill directory"""
#     def get(self):
#         self.write({'files': os.listdir(self.server.local_dir)})


def worker_app(worker, **kwargs):
    app = web.Application(**kwargs)
    app['server'] = worker
    router = app.router
    router.add_get('/info.json', info_handler)
    router.add_get('/processing.json', processing_handler)
    router.add_get('/resources.json', resource_handler)
    router.add_get('/files.json', local_files_handler)
    router.add_get('/nbytes.json', nbytes_handler)
    router.add_get('/nbytes-summary.json', nbytes_summary)
    return app

# def HTTPWorker(worker, **kwargs):
#     application = MyApp(web.Application([
#         (r'/info.json', Info, {'server': worker}),
#         (r'/processing.json', Processing, {'server': worker}),
#         (r'/resources.json', Resources, {'server': worker}),
#         (r'/files.json', LocalFiles, {'server': worker}),
#         (r'/nbytes.json', NBytes, {'server': worker}),
#         (r'/nbytes-summary.json', NBytesSummary, {'server': worker})
#         ]), **kwargs)
#     return application
