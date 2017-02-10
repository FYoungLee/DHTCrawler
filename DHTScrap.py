import flatbencode
import logging
from threading import Thread
from multiprocessing import Process
from Nodes import *
from tools import *

BOOTSTRAP_NODES = (
    ('router.bittorrent.com', 6881),
    ('dht.transmissionbt.com', 6881),
    ('router.utorrent.com', 6881),
)

Find_Node_Delay = 0.1

class DHTSpider(Process):
    def __init__(self):
        super().__init__()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # self.myid = hashnid() # this property is needed when actually download some real torrents.
        self.routing_table = Leaf()
        self.announced = {}

    def get_init_nodes(self):
        for each_addr in BOOTSTRAP_NODES:
            msg = {b't': entropy(2), b'y': b'q', b'q': b'find_node',
                   b'a': {b'id': hashnid(), b'target': hashnid()}}
            self.socket.sendto(flatbencode.encode(msg), each_addr)

    def nodes_finder(self):
        msg = {b't': entropy(2), b'y': b'q', b'q': b'find_node', b'a': {}}
        while True:
            all_neighbors = self.routing_table.traverse()
            count = len(all_neighbors)
            if count == 0:
                self.get_init_nodes()
                continue
            for node in all_neighbors:
                msg[b'a'][b'id'] = get_closer(node.nid)
                msg[b'a'][b'target'] = get_closer(node.nid)
                self.socket.sendto(flatbencode.encode(msg), node.address)
                time.sleep(Find_Node_Delay)
            for infohash in self.announced:
                for node in self.announced[infohash]:
                    if node.bad:
                        self.announced[infohash].remove(node)

    def run(self):
        Thread(target=self.nodes_finder).start()
        while True:
            data, addr = self.socket.recvfrom(1024 * 2)
            try:
                resp = flatbencode.decode(data)
                if b'r' in resp and b'nodes' in resp[b'r']:
                    self.append_nodes(resp)
                elif b'q' in resp:
                    self.reply_node(resp, addr)
                elif b'y' in resp and b'e' in resp[b'y'] or b'q' not in resp:
                    pass
            except flatbencode.DecodingError:
                pass

    def append_nodes(self, resp):
        nodes_received = decode_nodes(resp[b'r'][b'nodes'])
        for each in nodes_received:
            rst = self.routing_table.append(Node(each[0], each[1], each[2]))
            if rst is 'Out of Range':
                logging.warning('<{}> {} is out of range.'.format(self.name, each))

    def reply_node(self, resp, addr):
        msg = None
        try:
            if b'ping' in resp[b'q']:
                logging.debug('<{}> {} {}'.format(self.name, addr, 'pinged me'))
                nid = resp[b'a'][b'id']
                msg = {b't': resp[b't'], b'y': b'r', b'r': {b'id': get_closer(nid)}}
                self.routing_table.append(Node(resp[b'a'][b'id'], addr[0], addr[1]))
            elif b'find_node' in resp[b'q']:
                logging.debug('<{}> {} {}'.format(self.name, addr, 'asked nodes'))
                nid = resp[b'a'][b'id']
                nodes = self.routing_table.get_neighbors(resp[b'a'][b'target'])
                neighbors = pack_nodes(nodes)
                msg = {b't': resp[b't'], b'y': b'r', b'r': {b'id': get_closer(nid), b'nodes': neighbors}}
                self.routing_table.append(Node(resp[b'a'][b'id'], addr[0], addr[1]))
            elif b'get_peers' in resp[b'q']:
                info_hash = resp[b'a'][b'info_hash']
                logging.debug('<{}> {} want: {}'.format(self.name, addr, info_hash))
                nodes = self.routing_table.get_neighbors(info_hash)
                neighbors = pack_nodes(nodes)
                msg = {b't': resp[b't'], b'y': b'r',
                       b'r': {b'id': get_closer(info_hash), b'token': entropy(4), b'nodes': neighbors}}
                if info_hash in self.announced:
                    nodes = self.announced[info_hash]
                    announcers = pack_peers(nodes)
                    msg[b'r'][b'values'] = announcers
                self.routing_table.append(Node(resp[b'a'][b'id'], addr[0], addr[1]))
            elif b'announce_peer' in resp[b'q']:
                nid = resp[b'a'][b'id']
                info_hash = resp[b'a'][b'info_hash']
                msg = {b't': resp[b't'], b'y': b'r', b'r': {b'id': get_closer(nid)}}
                p_port = resp[b'a'][b'port']
                if resp[b'a'][b'implied_port']:
                    p_port = addr[1]
                node = Node(resp[b'a'][b'id'], addr[0], addr[1], p_port)
                logging.info('<{}> {} had:  {} @port:{}'.format(self.name, addr, info_hash, p_port))
                self.routing_table.append(node)
                if info_hash in self.announced:
                    if node not in self.announced[info_hash]:
                        self.announced[info_hash].append(node)
                else:
                    self.announced[info_hash] = [node, ]
        except KeyError:
            return
        if msg:
            self.socket.sendto(flatbencode.encode(msg), addr)


if __name__ == '__main__':
    logging.basicConfig(format='[%(asctime)s](%(levelname)s): %(message)s', datefmt=('%H:%M:%S'), level=logging.INFO)
    pool = []
    for each in range(5):
        dht = DHTSpider()
        dht.start()
        pool.append(dht)
    for each in pool:
        each.join()

