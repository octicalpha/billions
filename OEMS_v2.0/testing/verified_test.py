from verify import verified
from fastecdsa import ecdsa
from hashlib import sha384 as sha
import time

name = "tom"
timestamp = time.time()
priv_key = 0

message = sha("I {} am verified to trade at {}".format(name, timestamp)).hexdigest()
r, s = ecdsa.sign(message, priv_key, hashfunc=sha)

post = {
    'name': name,
    'timestamp': timestamp,
    'r': r,
    's': s
}

print(verified(post))
