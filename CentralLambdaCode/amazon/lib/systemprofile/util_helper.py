import math

def get_bytes(size, suffix):
    size = int(float(size))
    suffix = suffix.lower()

    if suffix == 'kb' or suffix == 'kib':
        return size << 10
    elif suffix == 'mb' or suffix == 'mib':
        return size << 20
    elif suffix == 'gb' or suffix == 'gib':
        return size << 30
    elif suffix == 'tb' or suffix == 'tib':
        return size << 40
    elif suffix == 'pb' or suffix == 'pib':
        return size << 50
    return False
