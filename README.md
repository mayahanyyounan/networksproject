# Project 2: RJDAP (Distributed Fleet Defense)

## 1. Project Overview

Project2 is a UDP-based distributed task processing system consisting of a central server and multiple worker nodes.

The server divides a large computational workload into smaller tasks and distributes them among available workers. Each worker processes its assigned task and sends the result back to the server.

The system uses a custom application-layer protocol to manage communication between the server and workers.

The main objective is to demonstrate distributed computing, UDP communication, task scheduling, reliability, and error detection.

## 2. System Architecture

The system consists of three main components:

### A. Protocol (protocol.py)

Defines the custom PC1 communication protocol.

Responsibilities:
- Defines message types and protocol version.
- Packs and unpacks network packets using Python's struct module.
- Calculates and verifies CRC32 checksums for error detection.
- Defines the payload formats for task dispatches and results.
- Maintains CSV logging functionality.

### B. Server (server.py)

The server coordinates the distributed processing system.

Responsibilities:
- Listens for incoming UDP packets.
- Registers workers and assigns worker IDs.
- Maintains a queue of pending tasks.
- Distributes tasks to available workers.
- Tracks assigned tasks and their attempts.
- Monitors worker activity using heartbeats.
- Detects task timeouts and reassigns unfinished tasks.
- Receives task results and sends acknowledgments.
- Tracks completed tasks and duplicate results.
- Collects performance statistics.

### C. Worker (worker.py)

Each worker is an independent computing node.

Responsibilities:
- Registers with the server.
- Sends periodic heartbeat messages.
- Receives and processes assigned tasks.
- Searches for a value whose SHA-256 hash matches the specified target prefix.
- Sends task results back to the server.
- Waits for acknowledgment packets.
- Retransmits results when acknowledgments are not received.
- Avoids recomputing tasks that have already been completed.

## 3. Message Types

The PC1 protocol defines the following message types:

| Message Type | Value | Description |
|---|---:|---|
| WORKER_JOIN | 1 | Worker requests registration. |
| HEARTBEAT | 2 | Indicates that a worker is active. |
| TASK_DISPATCH | 3 | Server assigns a task to a worker. |
| TASK_RESULT | 4 | Worker sends its computation result. |
| ACK_RESULT | 5 | Server acknowledges a received result. |

## 4. Packet Structure

Each PC1 packet consists of a fixed-size header followed by an optional payload.

The header is 20 bytes and contains:

- Version
- Message type
- Worker ID
- Task ID
- Attempt number
- Fragment index
- Total fragments
- CRC32 checksum
- Timestamp
- Flags

The payload contains additional information depending on the message type.

CRC32 is calculated over the header (with the checksum field set to zero) and the payload. The receiver recalculates the checksum to detect corrupted packets.

## 5. Task Processing

The server initially creates a queue of 1000 tasks.

Each task contains a numerical search range and a target hash prefix.

When a worker receives a TASK_DISPATCH message, it:
1. Extracts the assigned search range and target prefix.
2. Computes SHA-256 hashes for the numbers in the range.
3. Checks whether a hash matches the target prefix.
4. Sends the result to the server.
5. Waits for an ACK_RESULT message.

The server records completed tasks and assigns new tasks to available workers.

## 6. Reliability Mechanisms

Although UDP does not guarantee reliable delivery, the PC1 system implements several mechanisms to improve reliability:

**1. CRC32 Checksum**

Detects corrupted packets during transmission.

**2. Result Acknowledgment**

The server sends an ACK_RESULT message after receiving a task result.

**3. Retransmission**

Workers retransmit task results if an acknowledgment is not received.

**4. Exponential Backoff**

The retransmission timeout increases after each unsuccessful attempt, up to a maximum timeout.

**5. Task Timeout and Reassignment**

If a worker does not return a result within the specified timeout, the server puts the task back into the queue and assigns it again.

**6. Duplicate Detection**

The server tracks completed tasks to avoid counting the same result more than once. Workers also maintain a cache of previously computed tasks to avoid unnecessary recomputation.

**7. Heartbeat Monitoring**

Workers periodically send heartbeat messages so the server can monitor their activity.

## 7. Performance Monitoring and Logging

The system collects performance information through CSV log files.

Server:
- server_log.csv

Workers:
- worker_1_log.csv
- worker_2_log.csv
- Additional worker log files depending on the worker ID.

The logs record packet transmission and reception information, including timestamps, message types, task IDs, attempts, and packet sizes.

The server also tracks:
- Total completion time.
- Number of reassigned tasks.
- Number of duplicate results dropped.
- Number of corrupted packets.

Each worker tracks:
- Number of tasks computed.
- Number of duplicate dispatches.
- Number of result retransmissions.
- Number of tasks abandoned.
- Computation time.
- Idle time.

## 8. Technologies Used

- Python 3
- UDP Sockets
- Python struct module
- SHA-256 hashing
- CRC32 error detection
- Multithreading
- CSV logging

## 9. Project Files

| File | Description |
|---|---|
| protocol.py | Defines the PC1 protocol and packet handling functions. |
| server.py | Implements the central server and task scheduler. |
| worker.py | Implements the distributed worker nodes. |

## 10. Running the Project

Make sure Python 3 is installed and all three Python files are in the same directory.

### Step 1: Start the Server

Open a terminal and run:

python server.py

The server listens on UDP port 12000.

### Step 2: Start Workers

Open separate terminals for each worker and run:

python worker.py 127.0.0.1 12000 1

For additional workers:

python worker.py 127.0.0.1 12000 2

python worker.py 127.0.0.1 12000 3

For testing across different machines, replace 127.0.0.1 with the server machine's IP address.

### Step 3: Monitor Execution

The server distributes tasks to available workers and collects their results.

CSV log files are generated to record communication and performance information.

## 11. Conclusion

The PC1 project demonstrates how a distributed task processing system can be implemented using UDP sockets and a custom application-layer protocol.

It introduces mechanisms for packet validation, task scheduling, worker monitoring, retransmission, acknowledgment, timeout handling, and performance evaluation.
