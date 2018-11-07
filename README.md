# Server for ROSS-Vis app
App server with streaming data support for developing data analytics and visualization applications to analyze the performance of the ROSS simulator engine.

## Requirement
Python version => 3.4

## Install
```
pip install -r requirements.txt
```

## Start Server
```
python appserver.py --http=8888 --stream=8000
```

The app server is listening HTTP and WebSocket requests on port 8888 and receiving data streams on port 8000

