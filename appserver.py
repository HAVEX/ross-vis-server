import logging
import tornado.escape
import tornado.ioloop
import tornado.options
import tornado.web
import tornado.websocket
import os.path
import uuid
import struct

from tornado.options import define, options
from tornado.tcpserver import TCPServer
from tornado.iostream import StreamClosedError

from streamserver import DataStreamHandler, startSocketServer
from datareader import readRossDataSample, getSampleSize

define("http", default=8888, help="run on the given port", type=int)
define("stream", default=8000, help="streaming on the given port", type=int)

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [(r"/", MainHandler), (r"/websocket", WebSocketHandler)]
        settings = dict(
            cookie_secret="__TODO:_GENERATE_YOUR_OWN_RANDOM_VALUE_HERE__",
            template_path=os.path.join(os.path.dirname(__file__), "app"),
            static_path=os.path.join(os.path.dirname(__file__), "app"),
            xsrf_cookies=True,
        )
        super(Application, self).__init__(handlers, **settings)


class StreamServer(TCPServer):
    async def handle_stream(self, stream, address):
        while True:
            try:
                sizeBuf = await stream.read_bytes()
                size = getSampleSize(sizeBuf)
                data = await stream.read_bytes(size)
                print(size, len(data))
            except StreamClosedError:
                break

class WebSocketHandler(tornado.websocket.WebSocketHandler):
    waiters = set()
    cache = []
    cache_size = 200

    def open(self):
        print('new connection')
        self.write_message("Hello")
      
    def on_message(self, message):
        print('message received %s' % message)
        ChatSocketHandler.waiters.add(self)
 
    def on_close(self):
        print('connection closed')
        ChatSocketHandler.waiters.remove(self)


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.render("index.html")

def processor(sample):
    data = readRossDataSample(sample)
    print(data)


def main():
    tornado.options.parse_command_line()
    app = Application()
    app.listen(options.http)

    server = StreamServer()
    server.listen(options.stream)
    # startSocketServer(DataStreamHandler, 'localhost', options.stream)
    print("HTTP and WebSocket listening on", 'localhost', options.http)
    DataStreamHandler.setDataProcessor(processor)
    tornado.ioloop.IOLoop.current().start()    


if __name__ == "__main__":
    main()
