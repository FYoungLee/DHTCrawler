import time
from bitstring import BitArray
from struct import pack
import socket
from tools import *

K = 8

class Leaf(list):
    def __init__(self, _min=0, _max=2**160):
        super().__init__()
        self.Min = _min
        self.Max = _max
        self.L = None
        self.R = None
        self.leaf = True

    def append(self, p_object):
        if p_object in self:
            self[self.index(p_object)].refresh()
            return
        if self.leaf:
            if K <= len(self):
                self._split(p_object)
            else:
                super().append(p_object)
                self.sort()
        elif self.L.in_range(p_object):
            self.L.append(p_object)
        elif self.R.in_range(p_object):
            self.R.append(p_object)

    def _split(self, p_object):
        mid = (self.Min + self.Max) // 2
        self.L = Leaf(self.Min, mid)
        self.R = Leaf(mid, self.Max)
        for each in self:
            if self.L.in_range(each):
                self.L.append(each)
            else:
                self.R.append(each)
        if p_object.uint <= mid:
            self.L.append(p_object)
        else:
            self.R.append(p_object)
        self.clear()
        self.leaf = False

    def get_neighbors(self, nid):
        ret = []
        if self.leaf:
            ret.extend(self)
        elif self.L.in_range(nid):
            ret.extend(self.L.get_neighbors(nid))
            if len(ret) != K:
                ret.extend(self.R._more_neighbors('L', K-len(ret)))
        elif self.R.in_range(nid):
            ret.extend(self.R.get_neighbors(nid))
            if len(ret) != K:
                ret.extend(self.L._more_neighbors('R', K - len(ret)))
        return ret

    def _more_neighbors(self, side, num):
        ret = []
        if self.leaf:
            if 'L' is side:
                ret.extend(self[:num])
            else:
                ret.extend(self[-num:])
        elif 'L' is side:
            ret.extend(self.L._more_neighbors('L', num))
            if len(ret) < num:
                ret.extend(self.R._more_neighbors('L', num-len(ret)))
        elif 'R' is side:
            ret.extend(self.R._more_neighbors('R', num))
            if len(ret) < num:
                ret.extend(self.L._more_neighbors('R', num-len(ret)))
        return ret

    def in_range(self, obj):
        if isinstance(obj, Node):
            return self.Min < obj.uint <= self.Max
        elif isinstance(obj, bytes):
            return self.Min < BitArray(obj).uint <= self.Max

    def traverse(self):
        ret = []
        if self.leaf:
            for node in self:
                if not node.bad:
                    ret.append(node)
                else:
                    self.remove(node)
        else:
            ret.extend(self.L.traverse())
            ret.extend(self.R.traverse())
            if self._empty_branch():
                self._become_leaf()
        return ret

    def _empty_branch(self):
        if self.L.leaf and len(self.L) == 0 and self.R.leaf and len(self.R) == 0:
            return True

    def _become_leaf(self):
        self.L = self.R = None
        self.leaf = True

class Node:
    def __init__(self, nid, address, ann_port=None):
        self.nid = nid
        self.address = address
        self.ann_port = ann_port if ann_port else address[1]
        self.last = time.time()

    @property
    def uint(self):
        return BitArray(self.nid).uint

    @property
    def bad(self):
        if time.time() - self.last <= 15 * 60:
            return False
        return True

    def __eq__(self, other):
        return self.nid == other.nid

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        return self.uint < other.uint

    def __repr__(self):
        return 'NodeID : {} ; Address : {}'.format(self.nid, self.address)

    def refresh(self):
        self.last = time.time()

    def nid_pack(self):
        return self.nid + socket.inet_aton(self.address[0]) + pack('>H', self.address[1])

    def addr_pack(self):
        return socket.inet_aton(self.address[0]) + pack('>H', self.address[1])

if __name__ == '__main__':
    node = Node(hashnid(), ('1.1.1.1', 1))
    print(node)
    print(node.nid_pack())
    print(node.addr_pack())

