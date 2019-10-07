# Server for ROSS-Vis app
App server with streaming data support for developing data analytics and visualization applications to analyze the performance of the ROSS simulator engine.

## Requirement
Python version => 3.6

## Install
```
pip install -r requirements.txt
```

## Start Server
To start the app server for listening HTTP and WebSocket requests on port 8888 and receiving data streams on port 8000:
```
python server.py --http=8888 --stream=8000
```

## Server (Streaming version)
1. Create ROSS-streaming parent folder
```
mkdir ROSS-streaming
cd ROSS-streaming
```

2. Export hpc-vast to the environment
```
export PYTHONPATH=/path/to/hpc-vast
```

3. Clone the client github repository ( https://github.com/HAVEX/ross-vis )

4. Install the required libraries and packages for running the client server (https://github.com/HAVEX/ross-vis/blob/master/README.md).

5. Point the simulation data (.bin) file created to a data folder (e.g., ROSS/data). 

6. Start the server
```
python3 server.py --http=8899 --datafile=/path/to/data/df-amg1728-8pe-16kp-100gvt.bin --appdir=../ross-vis/dist
```

7. Go to localhost:XXXX. XXXX is specified by the client server. 
