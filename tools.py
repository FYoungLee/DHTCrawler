from random import randint, choice
from hashlib import sha1
import socket
from struct import pack, unpack
from bitstring import BitArray

def entropy(length):
    return ''.join(chr(randint(0, 255)) for _ in range(length)).encode()


def hashnid(length=20):
    h = sha1()
    h.update(entropy(length))
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

