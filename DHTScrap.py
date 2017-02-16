import flatbencode
from threading import Thread
from multiprocessing import Process, Queue
from Nodes import *
from tools import *

Find_Node_Delay = 0.1


class DHTSpider(Process):
    def __init__(self, infohash=None, tasks=None):
        super().__init__()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(('', randint(6000,10000)))
        self.myid = randomnid() # this property is needed when actually download some real torrents.
        self.target_infohash = infohash
        self.routing_table = Leaf()
        self.announced = {}
        self.syn_record = {}
        self.tasks = tasks if tasks else Queue()

    def get_init_nodes(self):
        for each_addr in BOOTSTRAP_NODES:
            msg = {b't': randombytes(2), b'y': b'q', b'q': b'find_node',
                   b'a': {b'id': randomnid(), b'target': randomnid()}}
            self.socket.sendto(flatbencode.encode(msg), each_addr)

    def msg_sender(self):
        if self.target_infohash:
            msg = {b"t": randombytes(2), b"y": b"q", b"q": b"get_peers",
                   b"a": {b"id": self.myid, b"info_hash": self.target_infohash}}
        else:
            msg = {b't': randombytes(2), b'y': b'q', b'q': b'find_node', b'a': {}}
        while True:
            all_neighbors = self.routing_table.traverse()
            count = len(all_neighbors)
            if count == 0:
                self.get_init_nodes()
                continue
            for node in all_neighbors:
                if not self.target_infohash:
                    msg[b'a'][b'id'] = get_closer(node.nid)
                    msg[b'a'][b'target'] = get_closer(node.nid)
                self.socket.sendto(flatbencode.encode(msg), node.address)
                # self.socket.sendto(self.utp_header(65), node.address)
                time.sleep(Find_Node_Delay)
            for infohash in self.announced:
                for node in self.announced[infohash]:
                    if node.bad:
                        self.announced[infohash].remove(node)

    def hash_requester(self):
        # TODO hash request funciton
        pass

    def run(self):
        Thread(target=self.msg_sender).start()
        if self.target_infohash:
            Thread(target=self.hash_requester).start()
        while True:
            data, addr = self.socket.recvfrom(1024)
            try:
                resp = flatbencode.decode(data)
                if b'r' in resp:
                    self.reply_handler(resp)
                elif b'q' in resp:
                    self.query_handler(resp, addr)
                elif b'y' in resp and b'e' in resp[b'y'] or b'q' not in resp:
                    pass
            except flatbencode.DecodingError:
                resp = UTP.decode_resp(data)
                self.utp_handler(resp, addr)

    def utp_handler(self, resp, addr):
        try:
            st_type, con_id, ts_differ, seq, ack, ext_data = resp
            if st_type == UTP.STATE:
                peer = self.syn_record[con_id]
                peer.seq_update(ack)
                msg = UTP.get_utp_header(UTP.DATA, peer.con_id, peer.ts_differ, seq_nr=peer.seq, ack_nr=peer.ack)
                msg += UTP.bt_handshake(peer)
                self.socket.sendto(msg, addr)
            elif st_type == UTP.DATA:
                logging.info('{} extend data :{}'.format(addr, ext_data))
                peer = self.syn_record[con_id]
                peer.seq_update(ack)
                msg = UTP.get_utp_header(UTP.FIN, peer.con_id, peer.ts_differ, seq_nr=peer.seq, ack_nr=peer.ack)
                self.socket.sendto(msg, addr)
                self.syn_record.pop(peer)
        except Exception as err:
            logging.critical('{} {} {}'.format(addr, resp, err))

    def reply_handler(self, resp):
        # logging.debug('{}'.format(resp))
        try:
            if b'nodes' in resp[b'r']:
                nodes_received = decode_nodes(resp[b'r'][b'nodes'])
                for each in nodes_received:
                    rst = self.routing_table.append(Node(each[0], each[1], each[2]))
                    if rst is 'Out of Range':
                        logging.warning('<{}> {} is an alien.'.format(self.name, each))
            if b'values' in resp[b'r']:
                peers = decode_peers(resp[b'r'][b'values'])
                for each in peers:
                    self.tasks.put(each)
        except KeyError:
            return

    def query_handler(self, resp, addr):
        try:
            reply_id = get_closer(resp[b'a'][b'id']) if not self.target_infohash else self.myid
            msg = {b't': resp[b't'], b'y': b'r', b'r': {b'id': reply_id}}
            self.routing_table.append(Node(resp[b'a'][b'id'], addr[0], addr[1]))
            if b'ping' in resp[b'q']:
                logging.debug('<{}> {} {}'.format(self.name, addr, 'pinged me'))
            elif b'find_node' in resp[b'q']:
                logging.debug('<{}> {} {}'.format(self.name, addr, 'asked nodes'))
                nodes = self.routing_table.get_neighbors(resp[b'a'][b'target'])
                neighbors = pack_nodes(nodes)
                msg[b'id'][b'nodes'] = neighbors
            elif b'get_peers' in resp[b'q']:
                info_hash = resp[b'a'][b'info_hash']
                logging.debug('<{}> {} want: {}'.format(self.name, addr, info_hash))
                msg[b'r'][b'id'] = get_closer(info_hash)
                nodes = self.routing_table.get_neighbors(info_hash)
                neighbors = pack_nodes(nodes)
                msg[b'r'][b'token'] = randombytes(4)
                msg[b'r'][b'nodes'] = neighbors
                if info_hash in self.announced:
                    nodes = self.announced[info_hash]
                    announcers = pack_peers(nodes)
                    msg[b'r'][b'values'] = announcers
                self.routing_table.append(Node(resp[b'a'][b'id'], addr[0], addr[1]))
            elif b'announce_peer' in resp[b'q']:
                info_hash = resp[b'a'][b'info_hash']

                # uTP contact procedure
                con_id = randint(1, 10000)
                msg = UTP.get_utp_header(UTP.SYN, con_id)
                self.socket.sendto(msg, addr)
                self.syn_record[con_id] = Peer(info_hash, con_id)

                # DHT reply procedure
                p_port = resp[b'a'][b'port']
                if resp[b'a'][b'implied_port']:
                    p_port = addr[1]
                node = Node(resp[b'a'][b'id'], addr[0], addr[1], p_port)
                logging.info('<{}> {} had:  {}'.format(self.name, addr, info_hash))
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
    logging.basicConfig(format='[%(asctime)s](%(levelname)s) Line %(lineno)d: %(message)s', datefmt=('%H:%M:%S'), level=logging.INFO)
    pool = []
    for each in range(3):
        dht = DHTSpider()
        dht.start()
        pool.append(dht)
    for each in pool:
        each.join()

