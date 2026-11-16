import struct, binascii , csv , os , time

PROTOCOL_NAME = "PC1"        # you still need to choose a unique name
VERSION = 1

# the server needs to know what kind of message it recieved 
#if message type =1, it knows a worker is joining 

WORKER_JOIN   = 1
HEARTBEAT     = 2
TASK_DISPATCH = 3
TASK_RESULT   = 4
ACK_RESULT    = 5

MSG_NAMES = {
    WORKER_JOIN: "WORKER_JOIN",
    HEARTBEAT: "HEARTBEAT",
    TASK_DISPATCH: "TASK_DISPATCH",
    TASK_RESULT: "TASK_RESULT",
    ACK_RESULT: "ACK_RESULT",
}

HEADER_FMT = '!BBHIBBBIIB'   
HEADER_SIZE = struct.calcsize(HEADER_FMT)   # 20
MAX_PAYLOAD = 512

def now_ms():
    return int(time.time() * 1000)

def now_ms16():
    return now_ms() & 0xFFFF

def pack_packet(msg_type, worker_id, task_id, attempt, timestamp,
                payload=b'',flags=0, frag_index=0, frag_total=1):
    if len(payload) > MAX_PAYLOAD:
        raise ValueError("payload too large")
    
    if not 0 <= flags <= 255:
       raise ValueError("flags must fit in one byte (0-255)")

    ts = timestamp & 0xFFFFFFFF        # keep it within 32 bits

    # 1) pack the header with checksum = 0 , this creates the 20 bytes
    header_zero = struct.pack(HEADER_FMT,VERSION, msg_type,
                              worker_id, task_id, attempt,
                              frag_index, frag_total, 0, ts,flags)

    # 2) CRC32 over header (checksum field = 0) + payload , joins the two bytes strings into one and turns them into one 32-bit number 
    checksum = binascii.crc32(header_zero + payload) & 0xFFFFFFFF # keeps thenumber withing 32 bits

    # 3) pack again with the real checksum
    header = struct.pack(HEADER_FMT, VERSION, msg_type,
                         worker_id, task_id, attempt,
                         frag_index, frag_total, checksum, ts, flags)

    return header + payload


def unpack_packet(data):
    # 1) too short to hold a header?
    if len(data) < HEADER_SIZE:
        return None , "too short"

    # 2) split the first 20 bytes into the 10 header fields
    (version, msg_type, worker_id, task_id, attempt,
     frag_index, frag_total, recv_checksum, ts,flags) = struct.unpack(
        HEADER_FMT, data[:HEADER_SIZE])

    # 3) everything after the header is the payload
    payload = data[HEADER_SIZE:]

    # 4) sanity checks
    if len(payload) > MAX_PAYLOAD:
        return None , "payload too large"
    if version != VERSION:
        return None , "bad version"
    if not (1 <= msg_type <= 5):
        return None , "unknown msg type"
    if frag_total < 1 or frag_index >= frag_total: #index have to be smaller than total
        return None , "bad fragment fields"

    # 5) checksum
    header_zero = struct.pack(HEADER_FMT, version, msg_type, worker_id,
                              task_id, attempt, frag_index, frag_total,
                              0, ts,flags)
    expected = binascii.crc32(header_zero + payload) & 0xFFFFFFFF
    if expected != recv_checksum:
        return None ,  "bad checksum"

    # 6) everything is fine
    return {
        "version": version, "msg_type": msg_type,
        "worker_id": worker_id, "task_id": task_id, "attempt": attempt,
        "frag_index": frag_index, "frag_total": frag_total,
        "timestamp": ts, "flags": flags ,"payload": payload,
    } , None

PREFIX_LEN = 4

DISPATCH_FMT = '!II%ds' % PREFIX_LEN       # '!II4s'
RESULT_FMT = '!BI'

DISPATCH_SIZE = struct.calcsize(DISPATCH_FMT)   # 4 + 4 + 4 = 12
RESULT_SIZE = struct.calcsize(RESULT_FMT)       # 1 + 4 = 5


def pack_dispatch_payload(range_start, range_end, target_prefix):
    # accept either an integer or bytes
    if isinstance(target_prefix, int):
        target_prefix = target_prefix.to_bytes(PREFIX_LEN, "big")
    if len(target_prefix) != PREFIX_LEN:
        raise ValueError("target_prefix must be %d bytes" % PREFIX_LEN)
    return struct.pack(DISPATCH_FMT, range_start, range_end, target_prefix)


def unpack_dispatch_payload(payload):
    if len(payload) != DISPATCH_SIZE:
        return None
    return struct.unpack(DISPATCH_FMT, payload)    # (start, end, prefix_bytes)  


def pack_result_payload(found, value):
    return struct.pack(RESULT_FMT, 1 if found else 0, value)


def unpack_result_payload(payload):
    if len(payload) != RESULT_SIZE:
        return None
    found, value = struct.unpack(RESULT_FMT, payload)
    return bool(found), value


LOG_FILE = "pc1.csv"
CSV_COLUMNS = ["time_ms", "direction", "peer", "msg_type", "worker_id",
               "task_id", "attempt", "payload_bytes", "note"]

def log_event(direction, addr, pkt, note="", log_file=LOG_FILE):
    t = int(time.time() * 1000)
    peer = f"{addr[0]}:{addr[1]}" if addr else ""

    if pkt is None:
        row = [t, direction, peer, "", "", "", "", "", note]
    else:
        name = MSG_NAMES.get(pkt["msg_type"], "UNKNOWN")
        row = [t, direction, peer, name, pkt["worker_id"], pkt["task_id"],
               pkt["attempt"], len(pkt["payload"]), note]

    print(*row)

    new_file = not os.path.exists(log_file) or os.path.getsize(log_file) == 0
    with open(log_file, "a", newline="") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(CSV_COLUMNS)
        w.writerow(row)


def log_sent(addr, packet, note="", log_file=LOG_FILE):
    d, _ = unpack_packet(packet)
    log_event("SEND", addr, d, note, log_file)


