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
from ross_vis.Transform import flatten, flatten_list

from cpdHandler import CPDHandler
from webSocketHandler import WebSocketHandler

define("http", default=8888, help="run on the given port", type=int)
define("stream", default=8000, help="streaming on the given port", type=int)
define("appdir", default="../app/dist", help="serving app in given directory", type=str)
define("datafile", default='', help="load data from file", type=str)

class Application(tornado.web.Application):
    def __init__(self, appdir = 'app'):
        handlers = [
            (r"/", MainHandler),
            (r'/app/(.*)', tornado.web.StaticFileHandler, {'path': appdir}),
            (r"/data", AjaxGetJsonData),
            (r"/websocket", WebSocketHandler),
            (r"/cpd", CPDHandler)
        ]
        settings = dict(
            cookie_secret="'a6u^=-sr5ph027bg576b3rl@#^ho5p1ilm!q50h0syyiw#zjxwxy0&gq2j*(ofew0zg03c3cyfvo'",
            template_path=os.path.join(os.path.dirname(__file__), appdir),
            static_path=os.path.join(os.path.dirname(__file__), appdir+'/static'),
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
                    logging.info('received and processed %d bytes', size)
                else:
                    print(size, len(data))
                WebSocketHandler.cache.push(data)

            except StreamClosedError:
                logging.info('stream connection closed')
                StreamServer.sources.remove(self)
                break

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.render("index.html")        
        
class AjaxGetJsonData(tornado.web.RequestHandler):
    def get(self):
        data = WebSocketHandler.cache.export_dict('KpData')
        print(data)
        schema = {k:type(v).__name__ for k,v in data[0].items()}
        self.write({
            'data': WebSocketHandler.cache.export_dict('KpData'),
            'schema': schema
        })

def main():
    tornado.options.parse_command_line()

    if (os.path.isfile(options.datafile)):
        WebSocketHandler.cache.loadfile(options.datafile)
        print('Test mode: loaded %d samples' % WebSocketHandler.cache.size())

    app = Application(options.appdir)
    app.listen(options.http)

    server = StreamServer()
    server.listen(options.stream)
    print("Receiving data streams on", 'localhost', options.stream)
    print("HTTP and WebSocket listening on", 'localhost', options.http)
    tornado.ioloop.IOLoop.current().start()    

if __name__ == "__main__":
    main()
