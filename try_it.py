from protocol import *

pkt = pack_packet(TASK_DISPATCH, worker_id=2, task_id=57, attempt=1,
                  timestamp=1234, payload=b'hello')
print("size:", len(pkt))
print("hex:", pkt.hex())
print("unpacked:", unpack_packet(pkt))
bad = bytearray(pkt)
bad[-1] ^= 0xFF
print("corrupted:", unpack_packet(bytes(bad)))

from protocol import *

payload = pack_dispatch_payload(0, 9999, 0xABCD)
print("dispatch payload:", payload.hex(), "len", len(payload))

pkt = pack_packet(TASK_DISPATCH, worker_id=2, task_id=57, attempt=1,
                  timestamp=1234, payload=payload)
print("packet len:", len(pkt))

got = unpack_packet(pkt)
print("task:", got["task_id"], "->", unpack_dispatch_payload(got["payload"]))

res = pack_result_payload(True, 4242)
print("result payload:", res.hex(), "->", unpack_result_payload(res))