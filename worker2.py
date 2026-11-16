"""RJDAP worker (compute node).

Usage: python worker.py <server_ip> <server_port> <worker_id> [quit_after_seconds]
Needs protocol.py (your pack/unpack code) in the same folder.
"""
import csv
import hashlib
import socket
import struct
import sys
import threading
import time

from protocol import (
    VERSION, WORKER_JOIN, HEARTBEAT, TASK_DISPATCH, TASK_RESULT, ACK_RESULT,
    pack_packet, unpack_packet,
    unpack_dispatch_payload, pack_result_payload,
)

TYPE_NAMES = {WORKER_JOIN: "JOIN", HEARTBEAT: "HEARTBEAT",
              TASK_DISPATCH: "DISPATCH", TASK_RESULT: "RESULT",
              ACK_RESULT: "ACK"}

# [CHANGED 1.0 to 0.8: sends heartbeats slightly faster than server's dead threshold]
HEARTBEAT_INTERVAL = 0.8    # seconds between heartbeats
# [CHANGED 0.2 to 0.3: accommodates up to 150ms network delay without premature retransmissions]
INITIAL_RTO = 0.3           # first wait for an ACK (seconds)
MAX_RTO = 2.0               # backoff ceiling
MAX_TRIES = 6               # retransmit cap -> never retransmit forever


def now_ms16():
    """Current time in milliseconds, wrapped to 16 bits (header field)."""
    return int(time.time() * 1000) & 0xFFFF


def compute(range_start, range_end, target_prefix):
    """Find the first n in [start, end) whose SHA-256 starts with the
    16-bit target prefix. The server MUST use the same rule.
    Returns (found, value)."""
    # [NEW: unpack_dispatch_payload returns bytes; cast to int & mask 16-bits to fix 'int == bytes' bug]
    if isinstance(target_prefix, bytes):
        target_prefix = int.from_bytes(target_prefix, 'big')
    prefix16 = target_prefix & 0xFFFF

    for n in range(range_start, range_end):
        digest = hashlib.sha256(struct.pack('!I', n)).digest()
        # [CHANGED target_prefix to prefix16: fixes comparison so target matches 16-bit prefix]
        if int.from_bytes(digest[:2], 'big') == prefix16:
            return True, n
    return False, 0


class Worker:
    def __init__(self, server, worker_id, quit_after=10.0):
        self.server = server
        self.wid = worker_id
        self.quit_after = quit_after

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(0.05)       # short, so loops can check timers

        self.done = {}                   # task_id -> (found, value): dedup cache
        self.pending = []                # dispatches that arrived while waiting for an ACK
        self.stop = threading.Event()

        # metrics
        self.idle_ms = 0.0
        self.compute_ms = 0.0
        self.tasks_computed = 0
        self.duplicate_dispatches = 0
        self.retransmissions = 0
        self.gave_up = 0

        self.log_file = open(f"worker_{worker_id}_log.csv", "w", newline="")
        self.log = csv.writer(self.log_file)
        self.log.writerow(["time", "direction", "type", "task_id", "attempt", "bytes"])
        self.log_lock = threading.Lock()

    # ---------- logging / socket helpers ----------

    def _log(self, direction, msg_type, task_id, attempt, size):
        with self.log_lock:
            self.log.writerow([f"{time.time():.6f}", direction,
                               TYPE_NAMES.get(msg_type, msg_type),
                               task_id, attempt, size])
            # [NEW: flush immediately so CSV data is written without buffering delay]
            self.log_file.flush()

    def send(self, msg_type, task_id=0, attempt=0, payload=b'', flags=0):
        pkt = pack_packet(msg_type, self.wid, task_id, attempt,
                          now_ms16(), payload, flags)
        self.sock.sendto(pkt, self.server)
        self._log("SEND", msg_type, task_id, attempt, len(pkt))

    def recv(self):
        """Return one valid parsed packet, or None (timeout / corrupt / wrong version).
        Corrupted packets are dropped silently, exactly like a lost packet."""
        try:
            data, _addr = self.sock.recvfrom(2048)
        except socket.timeout:
            return None
        except ConnectionResetError:     # Windows: ICMP 'port unreachable' noise
            return None
        pkt = unpack_packet(data)
        if pkt is None or pkt["version"] != VERSION:
            return None
        self._log("RECV", pkt["msg_type"], pkt["task_id"], pkt["attempt"], len(data))
        return pkt

    # ---------- protocol steps ----------

    def join(self):
        """Send WORKER_JOIN until the server answers.
        This server answers a join with a HEARTBEAT (or TASK_DISPATCH),
        so we record the assigned worker_id and return."""
        while True:
            self.send(WORKER_JOIN)
            deadline = time.time() + 0.5
            while time.time() < deadline:
                pkt = self.recv()
                # [CHANGED: accept HEARTBEAT or TASK_DISPATCH, sync self.wid with server's assigned ID]
                if pkt and pkt["msg_type"] in (HEARTBEAT, TASK_DISPATCH):
                    self.wid = pkt["worker_id"]
                    if pkt["msg_type"] == TASK_DISPATCH:
                        self.pending.append(pkt)
                    return

    def heartbeat_loop(self):
        while not self.stop.is_set():
            self.send(HEARTBEAT)
            self.stop.wait(HEARTBEAT_INTERVAL)

    def send_result(self, task_id, attempt, found, value):
        """Send TASK_RESULT and wait for ACK_RESULT with retransmit + backoff.
        Returns True if acked, False if we gave up."""
        payload = pack_result_payload(found, value)
        rto = INITIAL_RTO
        for i in range(MAX_TRIES):
            if i > 0:
                self.retransmissions += 1
            self.send(TASK_RESULT, task_id, attempt, payload)
            deadline = time.time() + rto
            while time.time() < deadline:
                pkt = self.recv()
                if pkt is None:
                    continue
                if pkt["msg_type"] == ACK_RESULT and pkt["task_id"] == task_id:
                    return True
                if pkt["msg_type"] == TASK_DISPATCH:
                    self.pending.append(pkt)     # handle after this result is done
            rto = min(rto * 2, MAX_RTO)
        return False

    # ---------- main loop ----------

    def handle_dispatch(self, pkt):
        tid, att = pkt["task_id"], pkt["attempt"]

        if tid in self.done:
            # Server resent a task we already finished: do NOT recompute.
            self.duplicate_dispatches += 1
            found, value = self.done[tid]
        else:
            try:
                # [CHANGED: safe unpack dispatch payload]
                unpacked = unpack_dispatch_payload(pkt["payload"])
                if unpacked is None:
                    return
                start, end, prefix = unpacked
            except struct.error:
                return                              # malformed payload: ignore
            t0 = time.time()
            found, value = compute(start, end, prefix)
            self.compute_ms += (time.time() - t0) * 1000
            self.tasks_computed += 1
            self.done[tid] = (found, value)

        if not self.send_result(tid, att, found, value):
            self.gave_up += 1   # server will time out and reassign if needed

    def run(self):
        # [CHANGED: join() now handles registration directly; no blind append of HEARTBEAT]
        self.join()
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()

        last_activity = time.time()
        idle_start = time.time()
        try:
            while True:
                if self.pending:
                    pkt = self.pending.pop(0)
                else:
                    pkt = self.recv()
                    if pkt is None:
                        if time.time() - last_activity > self.quit_after:
                            break                   # server went quiet: all done
                        continue
                    if pkt["msg_type"] == TASK_DISPATCH:
                        self.idle_ms += (time.time() - idle_start) * 1000

                last_activity = time.time()
                if pkt["msg_type"] != TASK_DISPATCH:
                    continue

                self.handle_dispatch(pkt)
                idle_start = time.time()
        except KeyboardInterrupt:
            pass
        finally:
            self.stop.set()
            self.summary()
            self.log_file.close()

    def summary(self):
        print(f"[worker {self.wid}] tasks computed      : {self.tasks_computed}")
        print(f"[worker {self.wid}] duplicate dispatches: {self.duplicate_dispatches}")
        print(f"[worker {self.wid}] result retransmits  : {self.retransmissions}")
        print(f"[worker {self.wid}] gave up on          : {self.gave_up}")
        print(f"[worker {self.wid}] compute time (ms)   : {self.compute_ms:.1f}")
        print(f"[worker {self.wid}] idle time (ms)      : {self.idle_ms:.1f}")


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("usage: python worker.py <server_ip> <server_port> <worker_id> [quit_after_s]")
        sys.exit(1)
    quit_after = float(sys.argv[4]) if len(sys.argv) > 4 else 10.0
    Worker((sys.argv[1], int(sys.argv[2])), int(sys.argv[3]), quit_after).run()
