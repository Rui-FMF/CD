# Echo client program
from datetime import datetime
import sys
import fcntl
import os
import socket
import json
import selectors
import time

HOST = 'localhost'      # Address of the host running the server  
PORT = 5000             # The same port as used by the server
USERNAME=''
connected=False
# set sys.stdin non-blocking
orig_fl = fcntl.fcntl(sys.stdin, fcntl.F_GETFL)
fcntl.fcntl(sys.stdin, fcntl.F_SETFL, orig_fl | os.O_NONBLOCK)

# function to be called when enter is pressed
def got_keyboard_data(socket,stdin):
    if connected==True:
        global USERNAME
        msg = stdin.read()
        if msg in '\r\n':
            desregister(socket)
            
        time_stamp = str(int(time.time()))
        data = json.dumps({"op" : "msg", "user" : USERNAME, "msg" : msg, "ts" : time_stamp})
        args=msg.split()
        if args[0]==":register":
            if len(args)>1:
                USERNAME=args[1]
                data = json.dumps({"op" : "register", "user" : USERNAME, "msg" : msg, "ts" : time_stamp})

        if args[0]==":deregister":
            desregister(socket)
        else:
            if USERNAME!='':
                s.send(data.encode('utf-8'))

def read(socket, stdin):
    data = (socket.recv(1024)).decode('utf-8')
    data = json.loads(data)
    stamp = str(datetime.fromtimestamp(int(data["ts"])))
    print('| '+ data["msg"].strip() + ' | sent on: ' + stamp)

def desregister(socket):
    global connected
    socket.close()
    print("Saíste do chat")
    connected=False

def registerr():
    s.connect((HOST, PORT))
    s.setblocking(False)
    selector = selectors.DefaultSelector()
    selector.register(sys.stdin, selectors.EVENT_READ, got_keyboard_data)
    selector.register(s,selectors.EVENT_READ,read)
    connected=True
    while True:
        # print('Type something and hit enter: ', end='', flush=True)
        for k, mask in selector.select():
            callback = k.data
            callback(s,k.fileobj)

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    s.setblocking(False)
    # register event
    selector = selectors.DefaultSelector()
    selector.register(sys.stdin, selectors.EVENT_READ, got_keyboard_data)
    selector.register(s,selectors.EVENT_READ,read)
    connected=True
    while True:
        # print('Type something and hit enter: ', end='', flush=True)
        for k, mask in selector.select():
            callback = k.data
            callback(s,k.fileobj)

