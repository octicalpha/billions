from fastecdsa import point, curve, ecdsa
from hashlib import sha384 as sha
from tinydb import TinyDB, Query

keys_db = TinyDB('public_keys.json')
all_keys = keys_db.all()
keys = {}
for key in all_keys:
    keys[key['name']] = point.Point(x=key['x'], y=key['y'], curve=curve.P256)

nonce_db = TinyDB('max_nonce.json')

query = Query()


def get_max_nonce(name):
    nonces = nonce_db.search(query.name == name)
    if len(nonces) == 0 or len(nonces) > 1:
        return None
    else:
        return nonces[0]['max_nonce']

def update_max_nonce(name, nonce):
    nonce_db.update({'name': name, 'max_nonce': nonce}, query.name == name)

def verified(post, logit=False):
    name = post['name']
    timestamp = float(post['timestamp'])
    max_nonce = get_max_nonce(name)

    if max_nonce is None:
        if logit:
            return 'nonce is None'
        else:
            return False

    if timestamp <= max_nonce:
        if logit:
            return 'timestamp <= {}'.format(max_nonce)
        else:
            return False

    update_max_nonce(name, timestamp)
    message_template = "I {} am verified to trade at {}".format(name, timestamp)
    pub_key = keys[name]
    r = long(post['r'])
    s = long(post['s'])
    message = sha(message_template).hexdigest()
    verified = ecdsa.verify((r, s), message, pub_key, hashfunc=sha)
    if verified:
        return True
    else:
        if logit:
            return 'not verified'
        else:
            return False
