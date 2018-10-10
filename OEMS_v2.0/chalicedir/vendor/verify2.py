from fastecdsa import point, curve, ecdsa
from hashlib import sha384 as sha
import os
from postgresConnection import query, storeInDb


def get_max_nonce(name):
    nonces = query("""select maxnonce from oms."max_nonce" where name = '{}'""".format(name))

    if len(nonces) == 0:
        rtn = 0.0
    else:
        rtn = max(long(x[0]) for x in nonces)
    return long(rtn)


def update_max_nonce(name, ip, timestamp):
    storeInDb([name, timestamp, ip], "max_nonce", "prod", schema="oms")


def get_pub_key(name):
    pub_key = query(""" select x,y,curve, in_z from oms."pubkey" where name = '{}' order by in_z desc""".format(name))
    if len(pub_key) == 0:
        return None
    else:
        return pub_key[0]


def verified(args, ip):
    try:
        name = args['name']
        timestamp = long(args['timestamp'])
        max_nonce = get_max_nonce(name)
        if timestamp <= max_nonce:
            return False

        r = long(args['r'])
        s = long(args['s'])
        pub_key = get_pub_key(name)
        if pub_key == None:
            return False

        x, y, curvenum, _ = pub_key
        pub_key = None
        if curvenum == 256:
            pub_key = point.Point(x=long(x), y=long(y), curve=curve.P256)
        update_max_nonce(name, ip, timestamp)

        message = sha("I {} am verified to trade at {}".format(name, timestamp)).hexdigest()
        return ecdsa.verify((r, s), message, pub_key, hashfunc=sha)
    except Exception as e:
        return str(e)
