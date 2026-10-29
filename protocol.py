import struct, binascii

VERSION = 1

WORKER_JOIN   = 1
HEARTBEAT     = 2
TASK_DISPATCH = 3
TASK_RESULT   = 4
ACK_RESULT    = 5

HEADER_FMT  = '!BBHIBHHH'
HEADER_SIZE = struct.calcsize(HEADER_FMT)   # 15
MAX_PAYLOAD = 512


def pack_packet(msg_type, worker_id, task_id, attempt, timestamp, payload=b'', flags=0):
    if len(payload) > MAX_PAYLOAD:
        raise ValueError("payload too large")

    ver_type = (VERSION << 4) | msg_type      # version in top 4 bits, type in bottom 4
    ts = timestamp & 0xFFFF                   # wrap to 16 bits

    # 1) pack with checksum = 0
    header_zero = struct.pack(HEADER_FMT, ver_type, flags, worker_id,
                              task_id, attempt, len(payload), ts, 0)

    # 2) compute checksum over header (with 0) + payload
    checksum = binascii.crc_hqx(header_zero + payload, 0)

    # 3) pack again with the real checksum
    header = struct.pack(HEADER_FMT, ver_type, flags, worker_id,
                         task_id, attempt, len(payload), ts, checksum)

    return header + payload

def unpack_packet(data):
    # 1) too short to be a real packet?
    if len(data) < HEADER_SIZE:
        return None

    # 2) split the first 15 bytes back into numbers
    (ver_type, flags, worker_id, task_id, attempt,
     payload_len, timestamp, checksum) = struct.unpack(HEADER_FMT, data[:HEADER_SIZE])

    version  = ver_type >> 4
    msg_type = ver_type & 0x0F
    payload  = data[HEADER_SIZE:]

    # 3) does the length field match the real payload?
    if payload_len != len(payload):
        return None

    # 4) checksum check (this replaces the three TODOs)
    header_zero = struct.pack(HEADER_FMT, ver_type, flags, worker_id,
                              task_id, attempt, payload_len, timestamp, 0)
    expected = binascii.crc_hqx(header_zero + payload, 0)
    if expected != checksum:
        return None

    # 5) everything is fine, return the fields
    return {
        "version": version, "msg_type": msg_type, "flags": flags,
        "worker_id": worker_id, "task_id": task_id, "attempt": attempt,
        "timestamp": timestamp, "payload": payload,
    }

DISPATCH_FMT = '!IIH'    # range_start, range_end, target_prefix
RESULT_FMT   = '!BI'     # found (0/1), value


def pack_dispatch_payload(range_start, range_end, target_prefix):
    return struct.pack(DISPATCH_FMT, range_start, range_end, target_prefix)


def unpack_dispatch_payload(payload):
    return struct.unpack(DISPATCH_FMT, payload)      # (start, end, prefix)


def pack_result_payload(found, value):
    return struct.pack(RESULT_FMT, 1 if found else 0, value)


def unpack_result_payload(payload):
    found, value = struct.unpack(RESULT_FMT, payload)
    return bool(found), value