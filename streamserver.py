import socketserver
import flatbuffers
import struct
import sys

class DataStreamHandler(socketserver.StreamRequestHandler):

    @classmethod
    def setDataProcessor(cls, processor):
        cls.processor = processor

    def handle(self):
        # self.request is the TCP socket connected to the client
        while True:
            self.data = self.request.recv(8 * 1024 * 1024).strip()

            if (len(self.data) == 0): # connection closed
                break

            bufSize = struct.unpack('i', self.data[:4])[0]
            print(len(self.data), bufSize)

            if(len(self.data) - bufSize == 4):
                if(callable(DataStreamHandler.processor)):
                    DataStreamHandler.processor(self.data)
                    

def startSocketServer(StreamHandler, host = 'localhost', port = 8000):
    server = socketserver.TCPServer((host, port), StreamHandler)
    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    print("Receiving data streams on", host, port)
    server.serve_forever()

if __name__ == "__main__":
    HOST = str(sys.argv[1])
    PORT = int(sys.argv[2])
    # DataStreamHandler.processData = sys.stdout.write
    startSocketServer(DataStreamHandler, HOST, PORT)