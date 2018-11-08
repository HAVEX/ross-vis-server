import logging
import tornado.escape
import tornado.ioloop
import tornado.options
import tornado.web
import tornado.websocket
import os.path
import uuid
import struct
import time

from tornado.options import define, options
from tornado.tcpserver import TCPServer
from tornado.iostream import StreamClosedError

from ross_vis.DataModel import RossData
from ross_vis.DataCache import RossDataCache
from ross_vis.Transform import flatten

define("http", default=8888, help="run on the given port", type=int)
define("stream", default=8000, help="streaming on the given port", type=int)
define("appdir", default="app", help="serving app in given directory", type=int)
define("test", default=0, help="run with test mode", type=int)

data_cache = RossDataCache()

class Application(tornado.web.Application):
    def __init__(self, appdir = 'app'):
        handlers = [
            (r"/", MainHandler),
            (r"/websocket", WebSocketHandler)
        ]
        settings = dict(
            cookie_secret="'a6u^=-sr5ph027bg576b3rl@#^ho5p1ilm!q50h0syyiw#zjxwxy0&gq2j*(ofew0zg03c3cyfvo'",
            template_path=os.path.join(os.path.dirname(__file__), appdir),
            static_path=os.path.join(os.path.dirname(__file__), appdir),
            xsrf_cookies=True,
        )
        
        super(Application, self).__init__(handlers, **settings)

class StreamServer(TCPServer):
    sources = set()
    cache = []
    cache_size = 100
    
    def set_data_handler(self, handler):
        if(callable(handler)):
            self.data_handler = handler

    async def handle_stream(self, stream, address):
        StreamServer.sources.add(self)
        while True:
            try:
                sizeBuf = await stream.read_bytes(RossData.FLATBUFFER_OFFSET_SIZE)
                size = RossData.size(sizeBuf)
                data = await stream.read_bytes(size)
                if(size == len(data)):
                    logging.info('recevied and processed %d bytes', size)
                
                WebSocketHandler.cache.append(data)

            except StreamClosedError:
                logging.info('stream connection closed')
                StreamServer.sources.remove(self)
                break


class WebSocketHandler(tornado.websocket.WebSocketHandler):
    waiters = set()
    cache = []
    cache_size = 100

    def open(self):
        print('new connection')
        WebSocketHandler.waiters.add(self)

    def on_message(self, message):
        print('message received %s' % message)
        if(message == 'test-streaming-data'):
            rd = RossData(['KpData'])
            for sample in data_cache.data:
                time.sleep(1)
                msg = {'data': flatten(rd.fetch(sample))}
                self.write_message(msg)

    def on_close(self):
        print('connection closed')
        WebSocketHandler.waiters.remove(self)

    @classmethod
    def push_updates(cls, data):
        for waiter in cls.waiters:
            try:
                waiter.write_message(data)
            except:
                logging.error("Error sending message", exc_info=True)        

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.render("index.html")

def main():
    tornado.options.parse_command_line()

    if(options.test == 1):
        data_cache.loadfile('data/data.bin')
        print('Test mode: loaded %d samples' % data_cache.size())

    app = Application(options.appdir)
    app.listen(options.http)

    server = StreamServer()
    server.listen(options.stream)
    print("Receiving data streams on", 'localhost', options.stream)
    print("HTTP and WebSocket listening on", 'localhost', options.http)
    tornado.ioloop.IOLoop.current().start()    


if __name__ == "__main__":
    main()
