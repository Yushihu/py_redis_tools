from hashlib import sha1 as _sha1


def sha1(script: str | bytes):
    if isinstance(script, str):
        script = script.encode('utf-8')
    return _sha1(script).hexdigest()
