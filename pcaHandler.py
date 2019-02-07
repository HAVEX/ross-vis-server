import tornado.web
import tornado.escape
import json
import numpy as np


from webSocketHandler import WebSocketHandler

class PCAHandler(tornado.web.RequestHandler):
    def get(self):
        return {}
