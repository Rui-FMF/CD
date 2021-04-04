import selectors
import socket
import json
import pickle
import xml.etree.ElementTree as ET

sel = selectors.DefaultSelector()

conn_types = {}         #stores serialization mechanisms of each consumer and producer
consumer_topics = {}    #stores the topics of each consumer queue
last_entry = {}         #stores the latest entry of each topic

def accept(sock, mask):
    conn, addr = sock.accept()
    print('accepted conection from', addr)
    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, read)

#method used to send data to the consumers
def send_to(consumer, topic, value):
    print("Sending data to "+conn_types[consumer]+" consumer")
    
    #convert data
    if conn_types[consumer] == "json":
        data = {"op":"pull_reply", "topic":topic, "value":value}
        data = (json.dumps(data)).encode("utf-8")
    elif conn_types[consumer] == "xml":
        data = "<data><op>pull_reply</op><topic>"+ str(topic) +"</topic><value>"+ str(value) +"</value></data>"
        data = data.encode("utf-8")
    elif conn_types[consumer] == "pickle":
        data = {"op":"pull_reply", "topic":topic, "value":value}
        data = pickle.dumps(data)

    header = str(len(data))
    header_size = len(header)
    header = 'h' * (4 - header_size) + header
    consumer.sendall(header.encode('utf-8') + data)

def read(conn, mask):
    header = conn.recv(4)
    header = header.decode('utf-8').replace('h','')

    if header:
        data = conn.recv(int(header))
    
        if data:

            #register if conn is a json, pickle or xml queue
            if conn not in conn_types:
                data = data.decode('utf-8')
                print("Registering conn type as "+data)
                conn_types[conn] = data
            else:
                #unpack the data
                print("Conn of type: "+conn_types[conn])
                if conn_types[conn] == "json":
                    data = json.loads(data.decode('utf-8'))
                elif conn_types[conn] == "xml":
                    data = data.decode('utf-8')
                    xml_tree = ET.ElementTree(ET.fromstring(data))
                    
                    #converting xml data to dictionary format
                    data = {}
                    for elem in xml_tree.iter():
                        data[elem.tag] = elem.text
                elif conn_types[conn] == "pickle":
                    data = pickle.loads(data)

                #check operation
                print("OPERATION: "+data["op"])
                if data["op"] == "subscribe":
                    if conn not in consumer_topics:                     #if a consumer has no topic yet, then add it to the consumer_topics dicc
                        consumer_topics[conn] = [data["topic"]]
                    elif data["topic"] not in consumer_topics[conn]:    #if a consumer has a topic and it's not subscribing to the same one
                        consumer_topics[conn].append(data["topic"])     #then add the new topic to the list of topics

                    print("Consumer subscribed to: "+data["topic"])
                    if data["topic"] in last_entry:
                        #send last entry in the topic to newly subscribed consumer
                        send_to(conn,data["topic"],last_entry[data["topic"]])

                elif data["op"] == "publish":
                    topics = data["topic"].split("/")
                    print("Publishing:"+str(data["value"])+" to topic: "+data["topic"])
                    for consumer, topic_list in consumer_topics.items():
                        for topic in topic_list:                                    #Send published data to all consumers subscribed
                            temp_topic = "/"                                        #to the topic or a parent topic
                            if temp_topic==topic:                                   #Example, if publishing to: /A/B/C
                                send_to(consumer,data["topic"],data["value"])       #The data would be sent to any consumers subscribed to the topics:
                            for t in topics[1:]:                                    # /A/B/C , /A/B , /A and / this last one beeing the root
                                temp_topic+=t
                                if temp_topic==topic:
                                    send_to(consumer,data["topic"],data["value"])
                                temp_topic+="/"

                    last_entry[data["topic"]] = data["value"]   #update topic's last entry
                
                elif data["op"] == "unsubscribe":
                    if data["topic"] in consumer_topics[conn]:
                        print("Consumer unsubscribing from: "+data["topic"])
                        consumer_topics[conn].remove(data["topic"])
                        if len(consumer_topics[conn])==0:               #If a consumer is left with 0 topics the conection is closed
                            print('closing', conn)                      #to avoid having a consumer pull with no topics
                            sel.unregister(conn)                        #which would leave it stuck trying to receive
                            conn.close()
                elif data["op"] == "list":
                    print("Sending list of registered topics")
                    for topic in last_entry:
                        send_to(conn,topic,"")
                    send_to(conn,"done","")


    else:
        print('closing', conn)
        sel.unregister(conn)
        conn.close()


sock = socket.socket()
sock.bind(('localhost', 8000))
sock.listen(100)
sock.setblocking(False)
sel.register(sock, selectors.EVENT_READ, accept)

while True:
    events = sel.select()
    for key, mask in events:
        callback = key.data
        callback(key.fileobj, mask)
