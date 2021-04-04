# Echo server program
import selectors
import socket
import json

HOST = ''                 # Symbolic name meaning all available interfaces
PORT = 5000               # Arbitrary non-privileged port

sel = selectors.DefaultSelector()

clients = {}

class Package:
  def __init__(self, data):
      data = json.loads(data.decode('utf-8'))
      self.op = data["op"]
      self.user = data["user"]
      self.msg = data["msg"]
      self.ts = data["ts"]

def accept(sock, mask):
    conn, addr = sock.accept()  # Should be ready
    print('accepted', conn, 'from', addr)
    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, read)

def read(conn, mask):
    data = conn.recv(1000)  # Should be ready
    if data:
        pack = Package(data)
        if pack.op == "register":
            if pack.user in clients.values():
                print("ERROR: user already exists")
            else:
                clients[conn] = pack.user
                print("Registered user: " + pack.user)
        elif pack.op == "msg":
            if pack.user not in clients.values():
                print("ERROR: unregistered user")
            else:
                for client in clients:
                    print('echoing', pack.msg, 'to', client)
                    client.send(data)
    else:
        #Close connection
        print('closing', conn)
        print('deleting', clients[conn])
        clients.pop(conn)
        sel.unregister(conn)
        conn.close()

sock = socket.socket()
sock.bind((HOST, PORT))
sock.listen(100)
sock.setblocking(False)
sel.register(sock, selectors.EVENT_READ, accept)

while True:
    events = sel.select()
    for key, mask in events:
        callback = key.data
        callback(key.fileobj, mask)
