# coding: utf-8

import socket
import threading
import logging
import pickle
from utils import dht_hash, contains_predecessor, contains_successor
from Finger_Table import Finger_Table


class DHT_Node(threading.Thread):
    """ DHT Node Agent. """
    def __init__(self, address, dht_address=None, timeout=3):
        """ Constructor

        Parameters:
            address: self's address
            dht_address: address of a node in the DHT
            timeout: impacts how often stabilize algorithm is carried out
        """
        threading.Thread.__init__(self)
        self.id = dht_hash(address.__str__())
        self.addr = address #My address
        self.dht_address = dht_address  #Address of the initial Node
        if dht_address is None:
            self.inside_dht = True
            #I'm my own successor
            self.fingertable = Finger_Table(self.id, self.id, address, 10)   #Initializing Finger Table with node sucessor beeing itself
            self.predecessor_id = None                                       #Note: sucessor_id acessed with: self.fingertable.get(0)[0]
            self.predecessor_addr = None                                     #      sucessor_addr acessed with: self.fingertable.get(0)[1]
        else:
            self.inside_dht = False
            self.fingertable = Finger_Table(self.id, None, None, 10)         #Initializing Finger Table with sucessor beeing Null
            self.predecessor_id = None
            self.predecessor_addr = None
        self.keystore = {}  # Where all data is stored
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.settimeout(timeout)
        self.logger = logging.getLogger("Node {}".format(self.id))

    def send(self, address, msg):
        """ Send msg to address. """
        payload = pickle.dumps(msg)
        self.socket.sendto(payload, address)

    def recv(self):
        """ Retrieve msg payload and from address."""
        try:
            payload, addr = self.socket.recvfrom(1024)
        except socket.timeout:
            return None, None

        if len(payload) == 0:
            return None, addr
        return payload, addr

    def node_join(self, args):
        """ Process JOIN_REQ message.

        Parameters:
            args (dict): addr and id of the node trying to join
        """

        self.logger.debug('Node join: %s', args)
        addr = args['addr']
        identification = args['id']
        if self.id == self.fingertable.get(0)[0]: #I'm the only node in the DHT
            self.fingertable.insert(0, identification, addr)
            args = {'successor_id': self.id, 'successor_addr': self.addr}
            self.send(addr, {'method': 'JOIN_REP', 'args': args})
        elif contains_successor(self.id, self.fingertable.get(0)[0], identification):
            args = {'successor_id': self.fingertable.get(0)[0], 'successor_addr': self.fingertable.get(0)[1]}
            self.fingertable.insert(0, identification, addr)
            self.send(addr, {'method': 'JOIN_REP', 'args': args})
        else:
            self.logger.debug('Find Successor(%d)', args['id'])
            self.send(self.fingertable.get(0)[1], {'method': 'JOIN_REQ', 'args':args})
        self.logger.info(self)

    def notify(self, args):
        """ Process NOTIFY message.
            Updates predecessor pointers.

        Parameters:
            args (dict): id and addr of the predecessor node
        """

        self.logger.debug('Notify: %s', args)
        if self.predecessor_id is None or contains_predecessor(self.id, self.predecessor_id, args['predecessor_id']):
            self.predecessor_id = args['predecessor_id']
            self.predecessor_addr = args['predecessor_addr']
        self.logger.info(self)

    def stabilize(self, from_id, addr):
        """ Process STABILIZE protocol.
            Updates all successor pointers.

        Parameters:
            from_id: id of the predecessor of node with address addr
            addr: address of the node sending stabilize message
        """

        self.logger.debug('Stabilize: %s %s', from_id, addr)
        if from_id is not None and contains_successor(self.id, self.fingertable.get(0)[0], from_id):
            # Update our successor
            self.fingertable.insert(0, from_id, addr)

        # notify successor of our existence, so it can update its predecessor record
        args = {'predecessor_id': self.id, 'predecessor_addr': self.addr}
        self.send(self.fingertable.get(0)[1], {'method': 'NOTIFY', 'args':args})

    def put(self, key, value, address):
        """ Store value in DHT.

            Parameters:
            key: key of the data
            value: data to be stored
            address: address where to send ack/nack
        """
        key_hash = dht_hash(key)
        self.logger.debug('Put: %s %s', key, key_hash)
        #if my responsibility:
        if contains_predecessor(self.id, self.predecessor_id, key_hash):
            self.keystore[key] = value
            self.send(address, {'method': 'ACK'})
        #if next node's responsibility:
        elif contains_successor(self.id, self.fingertable.get(0)[0], key_hash):
            self.send(self.fingertable.get(0)[1], {'method': 'PUT', 'args':{'key':key, 'value': value, 'client_addr': address}})
        #send to closest preceding node to the id
        else:
            next_addr = self.fingertable.closestPrecedingNode(key_hash)
            self.send(next_addr, {'method': 'PUT', 'args':{'key':key, 'value': value, 'client_addr': address}})

    def get(self, key, address):
        """ Retrieve value from DHT.

            Parameters:
            key: key of the data
            address: address where to send ack/nack
        """
        key_hash = dht_hash(key)
        self.logger.debug('Get: %s %s', key, key_hash)
        #if my responsibility:
        if contains_predecessor(self.id, self.predecessor_id, key_hash):
            value = self.keystore[key]
            self.send(address, {'method': 'ACK', 'args': value})
        #if next node's responsibility:
        elif contains_successor(self.id, self.fingertable.get(0)[0], key_hash):
            self.send(self.fingertable.get(0)[1], {'method': 'GET', 'args':{'key':key, 'client_addr': address}})
        #send to closest preceding node to the id
        else:
            next_addr = self.fingertable.closestPrecedingNode(key_hash)
            self.send(next_addr, {'method': 'GET', 'args':{'key':key, 'client_addr': address}})

    def findsucc(self, id, finger_idx, origin):

        if self.predecessor_id is None or contains_predecessor(self.id, self.predecessor_id, id):
            self.send(origin, {'method': 'SUCC_REPLY', 'args': {'addr':self.addr, 'id':self.id, 'finger_idx':finger_idx}})
        elif contains_successor(self.id, self.fingertable.get(0)[0], id):
            self.send(origin, {'method': 'SUCC_REPLY', 'args': {'addr':self.fingertable.get(0)[1], 'id':self.fingertable.get(0)[0], 'finger_idx':finger_idx}})
        else:
            self.send(self.fingertable.get(0)[1], {'method': 'FIND_SUCC', 'args':{'id':id, 'finger_idx':finger_idx, 'origin':origin}})

    def run(self):
        self.socket.bind(self.addr)

        # Loop untiln joining the DHT
        while not self.inside_dht:
            join_msg = {'method': 'JOIN_REQ', 'args': {'addr':self.addr, 'id':self.id}}
            self.send(self.dht_address, join_msg)
            payload, addr = self.recv()
            if payload is not None:
                output = pickle.loads(payload)
                self.logger.debug('O: %s', output)
                if output['method'] == 'JOIN_REP':
                    args = output['args']
                    self.fingertable.insert(0, args['successor_id'], args['successor_addr'])
                    self.inside_dht = True
                    self.logger.info(self)

        done = False
        while not done:
            payload, addr = self.recv()
            if payload is not None:
                output = pickle.loads(payload)
                self.logger.info('O: %s', output)
                if output['method'] == 'JOIN_REQ':
                    self.node_join(output['args'])
                elif output['method'] == 'NOTIFY':
                    self.notify(output['args'])
                elif output['method'] == 'PUT':
                    if 'client_addr' in output['args']:
                        self.put(output['args']['key'], output['args']['value'], output['args']['client_addr'])
                    else:
                        self.put(output['args']['key'], output['args']['value'], addr)
                elif output['method'] == 'GET':
                    if 'client_addr' in output['args']:
                        self.get(output['args']['key'], output['args']['client_addr'])
                    else:
                        self.get(output['args']['key'], addr)
                elif output['method'] == 'PREDECESSOR':
                    # Reply with predecessor id
                    self.send(addr, {'method': 'STABILIZE', 'args': self.predecessor_id})
                elif output['method'] == 'STABILIZE':
                    # Initiate stabilize protocol
                    self.stabilize(output['args'], addr)
                elif output['method'] == 'FIND_SUCC':
                    # Initiate stabilize protocol
                    self.findsucc(output['args']['id'], output['args']['finger_idx'], output['args']['origin'])
                elif output['method'] == 'SUCC_REPLY':
                    # Initiate stabilize protocol
                    self.fingertable.insert(output['args']['finger_idx'], output['args']['id'], output['args']['addr'])
            else: #timeout occurred, lets run the stabilize algorithm
                # Ask successor for predecessor, to start the stabilize process
                self.send(self.fingertable.get(0)[1], {'method': 'PREDECESSOR'})
                #update next finger table index
                table_idx, generated_id = self.fingertable.getNextEntry(self.id)
                self.send(self.fingertable.get(0)[1], {'method': 'FIND_SUCC', 'args':{'id':generated_id, 'finger_idx':table_idx, 'origin':self.addr}})

    def __str__(self):
        return 'Node ID: {}; DHT: {}; Successor: {}; Predecessor: {}; FingerTable: {}'\
            .format(self.id, self.inside_dht, self.fingertable.get(0)[0], self.predecessor_id, self.fingertable)

    def __repr__(self):
        return self.__str__()
