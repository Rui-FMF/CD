from utils import contains_successor

class Finger_Table():
    def __init__(self, node_id, successor_id, successor_addr, size):
        self.node_id = node_id      #id of node were table belongs
        self.counter = 0            #counter for iterating the table
        self.table = []
        self.size = size
        for i in range(size):
            self.table.append((successor_id,successor_addr))
    
    def insert(self, idx, successor_id, successor_addr):
        self.table[idx] = (successor_id,successor_addr)

    def get(self, idx):
        return self.table[idx]

    def getNextEntry(self, node_id):
        generated_id = (node_id +(2**self.counter))%(2**self.size)
        table_idx = self.counter

        self.counter+=1
        if self.counter==self.size:
            self.counter=0

        return(table_idx,generated_id)

    def closestPrecedingNode(self, id):
        for entry in reversed(self.table):
            if contains_successor(self.node_id, id, entry[0]):
                return entry[1]

    def __str__(self):
        string = ""
        for x in self.table:
            string += str(x[0])
            string += ", " 
        return string