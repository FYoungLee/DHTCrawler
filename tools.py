from random import randint
from hashlib import sha1

def entropy(length):
    return ''.join(chr(randint(0, 255)) for _ in range(length)).encode()

def hashnid():
    h = sha1()
    h.update(entropy(20))
    return h.digest()