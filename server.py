import protocol
from socket import *
import time

serverPort= 12000
serverSocket=socket(AF_INET, SOCK_DGRAM)
serverSocket.bind(('' , serverPort)) #'': accept packets addressed to this machine on any available IPv4 interface
serverSocket.settimeout(0.02)  # recvfrom wakes up every 20 ms so we can check timeouts even when no packet arrives

TOTAL = 1000        # number of tasks the project requires
TIMEOUT = 0.2       # seconds to wait for a result before reassigning the task
DEAD_AFTER = 1.0    # seconds of silence before a worker is considered crashed

worker={} #stores each registered worker's ID, address, last_seen and which task it is busy with.
addrToId={}  # network address -> worker ID (so a repeated JOIN from the same worker gets the same ID)
nextWorkerId=1  # the server hands out unique worker IDs 1,2,3...

queue = list(range(1, TOTAL + 1))  # task IDs not yet dispatched (replaces nextTaskId)
pending = {}   # tasks sent but not finished: taskid -> {"worker": id, "sent": time}
attempts = {}  # taskid -> how many times it was dispatched
completedTasks = set()  # Stores IDs of tasks whose results have been processed.

startTime = None  # set when the first worker joins, used for total_completion_time

stats = {"reassigned": 0, "dup_dropped": 0, "corrupt": 0}  # metrics for the report

logFile = open("server_log.csv", "w")  # log of every packet sent/received (needed for the report)
logFile.write("time,direction,msg_type,worker_id,task_id,attempt\n")

def log(direction, msg_type, workerid, taskid, attempt):  # writes one line to the CSV log
 logFile.write("%.3f,%s,%d,%d,%d,%d\n" % (time.time(), direction, msg_type, workerid, taskid, attempt))

def now_ms():  # current time in milliseconds (the packet header wraps it to 16 bits)
 return int(time.time() * 1000)

def is_alive(wid):  # a worker is alive if we heard anything from it recently
 return time.time() - worker[wid]["last_seen"] < DEAD_AFTER

def send(msg_type, workerid, taskid, attempt, payload=b''):  # builds + sends + logs any packet
 message=protocol.pack_packet(msg_type, workerid, taskid, attempt, now_ms(), payload)
 serverSocket.sendto(message, worker[workerid]["addr"]) #worker[workerid]["addr"] is the address saved when the worker joined
 log("send", msg_type, workerid, taskid, attempt)

def send_task(taskid, workerid):  # builds and sends one TASK_DISPATCH
 attempts[taskid] = attempts.get(taskid, 0) + 1   # attempt no (1 = first try)
 start = taskid * 500                             # each task gets its own range
 payload = protocol.pack_dispatch_payload(start, start + 500, 25)
 send(protocol.TASK_DISPATCH, workerid, taskid, attempts[taskid], payload)
 pending[taskid] = {"worker": workerid, "sent": time.time()}
 worker[workerid]["busy"] = taskid                # remember this worker is working on it

def assign_idle():  # give the next queued task to every alive worker that is free
 for wid, w in worker.items():
   if queue and w["busy"] is None and is_alive(wid):
     send_task(queue.pop(0), wid)

def check_timeouts():  # tasks with no result for too long go back to the queue
 for taskid, p in list(pending.items()):
   if time.time() - p["sent"] > TIMEOUT:
     old = p["worker"]
     print("Timeout: task", taskid, "from worker", old, "-> reassigning")
     del pending[taskid]
     worker[old]["busy"] = None   # stop waiting on that worker (if its late result arrives, it is a duplicate)
     queue.insert(0, taskid)      # put the task at the FRONT of the queue
     stats["reassigned"] += 1

while len(completedTasks) < TOTAL: #stop when all tasks are done (was: while True)
 #recieve the packet and unpack it
 try:
   data,workerAddress=serverSocket.recvfrom(2048)
   packet=protocol.unpack_packet(data)
   if packet is None:  # corrupt/invalid packet, count it and ignore it instead of crashing
     stats["corrupt"] += 1
 except timeout:       # no packet arrived within 20 ms, that is fine
   packet=None

 if packet!=None:
   log("recv", packet["msg_type"], packet["worker_id"], packet["task_id"], packet["attempt"])

   #check worker join
   if packet["msg_type"] == protocol.WORKER_JOIN: #check if he recieve a workerjoin mssg
     if workerAddress in addrToId:      # same worker joined again (our reply was lost), keep its ID
       workerid = addrToId[workerAddress]
     else:                              # new worker, the SERVER assigns a unique ID
       workerid = nextWorkerId
       nextWorkerId += 1
       addrToId[workerAddress] = workerid
     if startTime is None:              # the clock for total_completion_time starts at the first JOIN
       startTime = time.time()
     worker[workerid] = {"addr": workerAddress, "last_seen": time.time(), "busy": None} #whenever a worker joins, save its ID and its network address so the server knows where to send its task.
     print("Worker registered:", workerid)
     # tell the worker which ID it was given (a HEARTBEAT packet whose worker_id field carries the new ID)
     send(protocol.HEARTBEAT, workerid, 0, 1)

   elif packet["worker_id"] in worker:  # from here on, only accept packets from registered workers
     workerid=packet["worker_id"]
     worker[workerid]["last_seen"] = time.time()   # any packet proves the worker is alive

     if packet["msg_type"] == protocol.HEARTBEAT:  # nothing else to do, last_seen is already updated
       pass

     elif packet["msg_type"]==protocol.TASK_RESULT:
       taskid=packet["task_id"] #packet is a dictionary and we want to retrieve taskid value
       #construct ack message (ALWAYS ack, even for duplicates, because our previous ACK may have been lost)
       send(protocol.ACK_RESULT, workerid, taskid, packet["attempt"])

       if taskid in completedTasks:       # already counted, this is a retransmission or a late result
         stats["dup_dropped"] += 1
       else:
         completedTasks.add(taskid)
         pending.pop(taskid, None)
         found, value = protocol.unpack_result_payload(packet["payload"])
         print("Task", taskid, "done by worker", workerid, "found:", found, "value:", value)

       if worker[workerid]["busy"] == taskid:  # the worker is free again
         worker[workerid]["busy"] = None

   # a dead worker is marked free so its tasks can time out and move to others
 check_timeouts()  # move timed-out tasks back to the queue
 assign_idle()     # give work to every free worker

totalTime = time.time() - startTime
print("All", TOTAL, "tasks completed")
print("total_completion_time: %.2f s" % totalTime)
print("reassigned:", stats["reassigned"], "duplicates dropped:", stats["dup_dropped"], "corrupt packets:", stats["corrupt"])
logFile.close()
