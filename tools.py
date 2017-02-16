from random import randint, choice
from hashlib import sha1
import socket
from struct import pack, unpack
from bitstring import BitArray
import logging
from datetime import datetime


BOOTSTRAP_NODES = (
    ('router.bittorrent.com', 6881),
    ('dht.transmissionbt.com', 6881),
    ('router.utorrent.com', 6881),
)




class UTP:
    SYN = 0b01000001
    DATA = 0b00000001
    STATE = 0b00100001
    RESET = 0b00110001
    FIN = 0b00010001
    ST_DICT = {0b01000001: 'ST_SYN',
               0b00000001: 'ST_DATA',
               0b00100001: 'ST_STATE',
               0b00110001: 'ST_RESET',
               0b00010001: 'ST_FIN'}

    @classmethod
    def get_type(cls, st_type):
        return cls.ST_DICT[st_type]

    @staticmethod
    def decode_resp(resp):
        assert len(resp) >= 20, 'incorrect data'
        try:
            utp_resp = unpack('>BBHIIIHH', resp[:20])
            st_type = utp_resp[0]
            con_id = utp_resp[2]
            ts_differ = abs(gettimeofday() - utp_resp[3])
            seq = utp_resp[-1]
            ack = utp_resp[-2]
            ext_data = resp[20:]
            return st_type, con_id, ts_differ, seq, ack, ext_data
        except Exception as err:
            logging.critical('{} decode error: {}'.format(resp, err))

    @staticmethod
    def get_utp_header(utype, conn_id, ts_differ=0, wnd=1024, seq_nr=None, ack_nr=0):
        seq_nr = seq_nr if seq_nr else randint(1, 65535)
        return pack('>BBHIIIHH', utype, 0, conn_id, gettimeofday(), ts_differ, wnd, seq_nr, ack_nr)

    @staticmethod
    def bt_handshake(peer):
        utp_header = b'\x13BitTorrent protocol'
        utp_header += bytes([0, 0, 0, 0, 0, 16, 0, 0])
        utp_header += peer.infohash
        utp_header += peer.pid
        return utp_header


def randombytes(length):
    return ''.join(chr(randint(0, 255)) for _ in range(length)).encode()


def randomnid(length=20):
    h = sha1()
    h.update(randombytes(length))
    return h.digest()


def get_closer(hashid):
    ba = BitArray(hashid)
    for index in range(randint(130, 155), 160):
        ba[index] = choice([True, False])
    return ba.bytes


def pack_neighbors(nbrs, type=1):
    if type:
        return [x.pack_nodes() for x in nbrs]
    else:
        return [x.pack_peers() for x in nbrs]


def pack_nodes(nodes):
    ret = []
    for node in nodes:
        ret.append(node.nid + socket.inet_aton(node.ip) + pack('>H', node.n_port))
    return ret


def pack_peers(nodes):
    ret = []
    for node in nodes:
        ret.append(socket.inet_aton(node.ip) + pack('>H', node.p_port))
    return ret


def decode_nodes(nodes):
    ret = []
    length = len(nodes)
    if (length % 26) != 0:
        return ret
    for i in range(0, length, 26):
        nid = nodes[i:i + 20]
        ip = socket.inet_ntoa(nodes[i + 20:i + 24])
        port = unpack('>H', nodes[i + 24:i + 26])[0]
        if port <= 0 or port > 2 ** 16:
            continue
        ret.append((nid, ip, port))
    return ret


def decode_peers(peers):
    ret = []
    for ipport in peers:
        try:
            ret.append((socket.inet_ntoa(ipport[:4]), unpack('>H', ipport[-2:])[0]))
        except Exception as err:
            logging.critical(err)
    return ret


def gettimeofday():
    yy = datetime.today().year
    mm = datetime.today().month
    dd = datetime.today().day
    return int((datetime.today().timestamp() - datetime(yy, mm, dd).timestamp()) * 1000)
