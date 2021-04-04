from enum import Enum
import socket
import selectors
import json
import pickle
import xml.etree.ElementTree as ET

class MiddlewareType(Enum):
    CONSUMER = 1
    PRODUCER = 2

class Queue:
    def __init__(self, topic, type=MiddlewareType.CONSUMER):
        self.topic = topic
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect(('localhost', 8000))

    #skeleton methods redefined in sub classes
    #------------------------------------------
    def serialize(self, data): 
        pass

    def unserialize(self, data):
        pass
    
    def build_data(self, op, topic="", value=""):
        pass
    #------------------------------------------

    def send_to_broker(self, data, informing=False):          #Informing is true if informing the broker of mech_type
        #converts data according to serialization mechanism
        if informing:
            data = data.encode('utf-8')
        else:
            data = self.serialize(data)
        
        header = str(len(data))
        header_size = len(header)
        header = 'h' * (4 - header_size) + header
        self.sock.sendall(header.encode('utf-8') + data)

    def push(self, value):
        data = self.build_data("publish",self.topic,value)
        self.send_to_broker(data)


    def pull(self):
        header = self.sock.recv(4)
        header = header.decode('utf-8').replace('h','')
        data = self.sock.recv(int(header))
        #unpack the data
        data = self.unserialize(data)
        return data["topic"],data["value"]

    def sub(self, topic):
        data = self.build_data("subscribe",topic)
        self.send_to_broker(data)
    
    def unsub(self, topic):
        data = self.build_data("unsubscribe",topic)
        self.send_to_broker(data)

    # Method that prints the existing topics in the broker (topics that have been published on)
    def list_topics(self):
        data = self.build_data("list")
        self.send_to_broker(data)

        print("Registered topic list:")         # In this implementation the list method prints the topics
        while True:                             # in the actual method, but alternatively they could be put inside
            topic, value = self.pull()          # a list and then returned to the consumer/producer
            if topic == "done":
                break
            print(topic)

class JSONQueue(Queue):
    def __init__(self, topic, type=MiddlewareType.CONSUMER):
        super().__init__(topic, type)
        self.send_to_broker("json", True)   #used to inform the broker of serialization mechanism
        if type==MiddlewareType.CONSUMER:
            self.sub(self.topic)
    
    def serialize(self, data):
        return (json.dumps(data)).encode('utf-8')

    def unserialize(self, data):
        return json.loads(data.decode('utf-8'))

    def build_data(self, op, topic="", value=""):
        data = {"op":op}
        if topic:
            data["topic"] = topic
            if str(value):
                data["value"] = value
        return data

class XMLQueue(Queue):
    def __init__(self, topic, type=MiddlewareType.CONSUMER):
        super().__init__(topic, type)
        self.send_to_broker("xml", True)
        if type==MiddlewareType.CONSUMER:
            self.sub(self.topic)

    def serialize(self, data):
        return data.encode('utf-8')

    def unserialize(self, data):
        xml_tree = ET.ElementTree(ET.fromstring(data.decode('utf-8')))            
        #converting xml data to dictionary format
        data = {}
        for elem in xml_tree.iter():
            data[elem.tag] = elem.text
        return data

    def build_data(self, op, topic="", value=""):
        data = "<data><op>"+op+"</op>"
        if topic:
            data = data + "<topic>"+ str(topic) +"</topic>"
            if str(value):
                data = data + "<value>"+ str(value) +"</value>"
        data = data + "</data>"
        return data

class PickleQueue(Queue):
    def __init__(self, topic, type=MiddlewareType.CONSUMER):
        super().__init__(topic, type)
        self.send_to_broker("pickle", True)
        if type==MiddlewareType.CONSUMER:
            self.sub(self.topic)

    def serialize(self, data):
        return pickle.dumps(data)

    def unserialize(self, data):
        return pickle.loads(data)

    def build_data(self, op, topic="", value=""):
        data = {"op":op}
        if topic:
            data["topic"] = topic
            if str(value):
                data["value"] = value
        return data