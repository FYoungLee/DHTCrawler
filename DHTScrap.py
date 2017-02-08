import flatbencode
import re
import logging
from struct import unpack
from threading import Thread
from multiprocessing import Process, Queue
from Nodes import *

BOOTSTRAP_NODES = (
    ('router.bittorrent.com', 6881),
    ('dht.transmissionbt.com', 6881),
    ('router.utorrent.com', 6881),
)


class DHTSpider(Process):
    def __init__(self):
        super().__init__()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.myid = hashnid()
        self.routetable = Leaf()
        self.announced = {}

    def get_init_nodes(self):
        for each_addr in BOOTSTRAP_NODES:
            msg = {b't': entropy(2), b'y': b'q', b'q': b'find_node',
                   b'a': {b'id': self.myid, b'target': self.myid}}
            self.socket.sendto(flatbencode.encode(msg), each_addr)

    def nodes_traverse(self):
        msg = {b't': entropy(2), b'y': b'q', b'q': b'find_node',
               b'a': {b'id': self.myid, b'target': self.myid}}
        while True:
            nodes = self.routetable.traverse()
            if not nodes:
                self.get_init_nodes()
            for node in nodes:
                self.socket.sendto(flatbencode.encode(msg), node.address)
                time.sleep(0.1)
            for infohash in self.announced:
                for node in self.announced[infohash]:
                    if node.bad:
                        self.announced[infohash].remove(node)
            time.sleep(randint(4, 8))

    def run(self):
        Thread(target=self.nodes_traverse).start()
        while True:
            data, addr = self.socket.recvfrom(1024 * 4)
            try:
                resp = flatbencode.decode(data)
                if b'r' in resp and b'nodes' in resp[b'r']:
                    recv_nodes = self.decode_nodes(resp[b'r'][b'nodes'])
                    for node in recv_nodes:
                        if self.check_node(node):
                            self.routetable.append(node)
                elif b'q' in resp:
                    msg = None
                    try:
                        if b'ping' in resp[b'q']:
                            logging.debug('{} {}'.format(addr, 'pinged me'))
                            msg = {b't': resp[b't'], b'y': b'r', b'r': {b'id': self.myid}}
                            self.routetable.append(Node(resp[b'a'][b'id'], addr))
                        elif b'find_node' in resp[b'q']:
                            logging.debug('{} {}'.format(addr, 'asked nodes'))
                            neighbors = self.routetable.get_neighbors(resp[b'a'][b'target'])
                            neighbors = self.pack_neighbors(neighbors)
                            msg = {b't': resp[b't'], b'y': b'r', b'r': {b'id': self.myid, b'nodes': neighbors}}
                            self.routetable.append(Node(resp[b'a'][b'id'], addr))
                        elif b'get_peers' in resp[b'q']:
                            info_hash = resp[b'a'][b'info_hash']
                            logging.info('{} {} {}'.format(addr, 'asked : ', BitArray(info_hash).hex.upper()))
                            if info_hash not in self.announced:
                                neighbors = self.routetable.get_neighbors(info_hash)
                                neighbors = self.pack_neighbors(neighbors)
                                msg = {b't': resp[b't'], b'y': b'r', b'r': {b'id': self.myid, b'token': entropy(4), b'nodes': neighbors}}
                            else:
                                announcers = self.announced[info_hash]
                                announcers = self.pack_neighbors(announcers, 0)
                                msg = {b't': resp[b't'], b'y': b'r', b'r': {b'id': self.myid, b'token': entropy(4), b'values': announcers}}
                            self.routetable.append(Node(resp[b'a'][b'id'], addr))
                        elif b'announce_peer' in resp[b'q']:
                            info_hash = resp[b'a'][b'info_hash']
                            logging.info('{} {} {}'.format(addr, 'announced : ', BitArray(info_hash).hex.upper()))
                            msg = {b't': resp[b't'], b'y': b'r', b'r': {b'id': self.myid}}
                            if resp[b'a'][b'implied_port']:
                                node = Node(resp[b'a'][b'id'], addr, resp[b'a'][b'port'])
                            else:
                                node = Node(resp[b'a'][b'id'], addr, addr[1])
                            self.routetable.append(node)
                            try:
                                self.announced[info_hash].append(node)
                            except KeyError:
                                self.announced[info_hash] = [node,]
                    except KeyError:
                        continue
                    if msg:
                        self.socket.sendto(flatbencode.encode(msg), addr)
                elif b'y' in resp and b'e' in resp[b'y'] or b'q' not in resp:
                    pass
            except flatbencode.DecodingError:
                pass

    @staticmethod
    def decode_nodes(nodes):
        n = []
        length = len(nodes)
        if (length % 26) != 0:
            return n
        for i in range(0, length, 26):
            nid = nodes[i:i + 20]
            ip = socket.inet_ntoa(nodes[i + 20:i + 24])
            port = unpack('>H', nodes[i + 24:i + 26])[0]
            n.append(Node(nid, (ip, port)))
        return n

    @staticmethod
    def pack_neighbors(nbrs, type=1):
        if type:
            return [x.nid_pack() for x in nbrs]
        else:
            return [x.addr_pack() for x in nbrs]

    @staticmethod
    def check_node(node):
        if len(node.nid) != 20:
            return False
        if not re.match(r'(\d+.){4}', node.address[0]):
            return False
        if node.address[1] <= 0 or node.address[1] > 2**16:
            return False
        return True


if __name__ == '__main__':
    logging.basicConfig(format='[%(asctime)s](%(levelname)s): %(message)s', datefmt=('%H:%M:%S'), level=logging.INFO)
    pool = []
    for each in range(8):
        dht = DHTSpider()
        dht.start()
        pool.append(dht)
    for each in pool:
        each.join()

