import sys
import tornado.ioloop
import tornado.web

from tornado.tcpserver import TCPServer
from tornado.iostream import StreamClosedError


class EchoServer(TCPServer):
    async def handle_stream(self, stream, address):
        while True:
            try:
                data = await stream.read_until(b"\n")
                await sys.stdout.write(data)
            except StreamClosedError:
                break


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")

def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
    ])

if __name__ == "__main__":
    app = make_app()
    app.listen(8888)
    server = TCPServer()
    server.listen(7000)
    tornado.ioloop.IOLoop.current().start()

    