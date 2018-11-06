import socketserver
import flatbuffers
import struct
import sys

def createDataStreamHandler(processData):
    class DataStreamHandler(socketserver.StreamRequestHandler):
        """
        The request handler class for our server.

        It is instantiated once per connection to the server, and must
        override the handle() method to implement communication to the
        client.
        """

        def handle(self):
            # self.request is the TCP socket connected to the client
            while True:
                self.data = self.request.recv(8 * 1024 * 1024).strip()

                if (len(self.data) == 0): # connection closed
                    break

                bufSize = struct.unpack('i', self.data[:4])[0]
                print(len(self.data), bufSize)

                if(len(self.data) - bufSize == 4):
                    if(callable(processData)):
                        processData(self.data)
                    

    return DataStreamHandler

def startSocketServer(streamHandler, host = 'localhost', port = 8000):
    server = socketserver.TCPServer((host, port), streamHandler)
    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    print("listening on", host, port)
    server.serve_forever()

if __name__ == "__main__":
    HOST = str(sys.argv[1])
    PORT = int(sys.argv[2])
    startSocketServer(createDataStreamHandler(sys.stdout.write), HOST, PORT)