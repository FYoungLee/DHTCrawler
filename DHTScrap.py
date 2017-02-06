import socket
import struct
import time
from random import randint
from hashlib import sha1
from flatbencode import encode, decode
import flatbencode
from struct import unpack, pack
from queue import deque, Queue
from bitstring import BitArray
from threading import Thread
from multiprocessing import Process, Queue

BOOTSTRAP_NODES = (
    ('router.bittorrent.com', 6881),
    ('dht.transmissionbt.com', 6881),
    ('router.utorrent.com', 6881),
)


class Node:
    def __init__(self, nid, ip, port):
        self.nid = nid
        self.ip = ip
        self.port = port

    def pack(self):
        return self.nid + socket.inet_aton(self.ip) + pack('>H', self.port)


class DHTSpider(Process):
    def __init__(self, info_hashes, nodes_table=None):
        super().__init__()
        self.info_hashes = info_hashes
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.raw_id = self.entropy(20)
        self.myid = self.hashid(self.raw_id)
        self.nodes_table = nodes_table if nodes_table else deque(maxlen=2**10)

    def sender(self, tasks, tasked):
        while True:
            msg, addr = tasks.get()
            if not addr[1]:
                continue
            try:
                self.socket.sendto(encode(msg), addr)
                # print(time.ctime(), msg, 'to', addr)
                tasked.add(addr)
            except OSError:
                print(addr, ' OS error!')
            time.sleep(0.2)

    @staticmethod
    def cleaner(tasked):
        while True:
            time.sleep(300)
            tasked.clear()

    def get_neighbors(self, target):
        def dist(node):
            distance = (BitArray(node.nid) ^ BitArray(target)).bin
            return distance, node

        ret = map(dist, self.nodes_table)
        ret = [x[1].pack() for x in sorted(ret, key=lambda x: x[0])[:8]]
        return b''.join(ret)

    def add_neighbors(self, node):
        if node in self.nodes_table:
            self.nodes_table.remove(node)
        self.nodes_table.append(node)

    def run(self):
        sendtask = Queue()
        tasked = set()
        for each_addr in BOOTSTRAP_NODES:
            msg = {b't': self.entropy(2), b'y': b'q', b'q': b'find_node',
                   b'a': {b'id': self.myid, b'target': self.myid}}
            sendtask.put((msg, each_addr))
        Thread(target=self.sender, args=(sendtask, tasked)).start()
        Thread(target=self.cleaner, args=(tasked,)).start()
        while True:
            data, addr = self.socket.recvfrom(1024 * 16)
            try:
                resp = decode(data)
                if b'r' in resp and b'nodes' in resp[b'r']:
                    msg = {b't': self.entropy(2), b'y': b'q', b'q': b'find_node', b'a': {b'id': self.myid, b'target': self.myid}}
                    recv_nodes = self.decode_nodes(resp[b'r'][b'nodes'])
                    for each in recv_nodes:
                        if (each[1], each[2]) not in tasked:
                            sendtask.put((msg, (each[1], each[2])))
                elif b'q' in resp:
                    if b'ping' in resp[b'q']:
                        print(time.ctime(), addr, 'pinged me.')
                        msg = {b't': resp[b't'], b'y': b'r', b'r': {b'id': self.myid}}
                    elif b'find_node' in resp[b'q']:
                        print(time.ctime(), addr, 'asked nodes')
                        neighbors = self.get_neighbors(resp[b'a'][b'target'])
                        msg = {b't': resp[b't'], b'y': b'r', b'r': {b'id': self.myid, b'nodes': neighbors}}
                    elif b'get_peers' in resp[b'q']:
                        print(time.ctime(), addr, 'asked info hash: ', resp[b'a'][b'info_hash'])
                        self.info_hashes.put((resp[b'a'][b'info_hash'], addr))
                        neighbors = self.get_neighbors(resp[b'a'][b'info_hash'])
                        msg = {b't': resp[b't'], b'y': b'r',
                               b'r': {b'id': self.myid, b'token': self.entropy(4), b'nodes': neighbors}}
                    elif b'announce_peer' in resp[b'q']:
                        print(time.ctime(), addr, 'announced info hash: ', resp[b'a'][b'info_hash'])
                        if resp[b'a'][b'implied_port'] == 1:
                            addr[1] = resp[b'a'][b'port']
                        self.info_hashes.put((resp[b'a'][b'info_hash'], addr))
                        msg = {b't': resp[b't'], b'y': b'r', b'r': {b'id': self.myid}}
                    else:
                        print(resp)
                        continue
                    if msg:
                        sendtask.put((msg, addr))
                        self.add_neighbors(Node(resp[b'a'][b'id'], addr[0], addr[1]))
                elif b'y' in resp and b'e' in resp[b'y'] or b'q' not in resp:
                    pass
            except flatbencode.DecodingError:
                pass

    @staticmethod
    def entropy(length):
        return ''.join(chr(randint(0, 255)) for _ in range(length)).encode()

    @staticmethod
    def hashid(nid):
        h = sha1()
        h.update(nid)
        return h.digest()

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
            n.append((nid, ip, port))
        return n


def make_handshake(infohash):
    return struct.pack('>h19sq', 19, b'BitTorrent protocol', 0) + infohash + DHTSpider.hashid(20)


def check_handshake(packet, self_infohash):
    try:
        bt_header_len, packet = ord(packet[:1]), packet[1:]
        if bt_header_len != len(b'BitTorrent protocol'):
            return False
    except TypeError:
        return False

    bt_header, packet = packet[:bt_header_len], packet[bt_header_len:]
    if bt_header != b'BitTorrent protocol':
        return False

    packet = packet[8:]
    infohash = packet[:20]
    if infohash != self_infohash:
        return False

    return True


def make_ext_handshake():
    return chr(20).encode() + chr(0).encode() + encode({b"m": {b"ut_metadata": 1}})


def request_metadata(ut_metadata, piece):
    """bep_0009"""
    return chr(20).encode() + chr(ut_metadata).encode() + encode({"msg_type": 0, "piece": piece})

if __name__ == '__main__':
    info_hashes = Queue()
    for each in range(5):
        dht = DHTSpider(info_hashes)
        dht.start()
    # so = socket.socket()
    # so.settimeout(5)
    # while True:
    #     t = info_hashes.get()
    #     try:
    #         so.connect(t[1])
    #         so.send(make_handshake(t[0]))
    #         print(time.ctime(), 'make handshake with', t[1])
    #         dat, addr = so.recv(1024)
    #         print(time.ctime(), addr, 'responsed handshake: ', dat)
    #         so.send(make_ext_handshake())
    #         print(time.ctime(), 'extend handshake with', t[1])
    #         dat, addr = so.recv(1024)
    #         print(time.ctime(), addr, 'responsed extend handshake :', dat)
    #     except:
    #         continue

