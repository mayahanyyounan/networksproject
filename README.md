# RJDAP_FLEET
## Reliable Job Distribution and Assignment Protocol

### 1. Project Overview

RJDAP_FLEET is a custom application-layer communication protocol designed to distribute computational tasks from a central Command Hub (server) to multiple worker nodes over UDP.

The system uses a reliable task distribution mechanism on top of UDP. It supports worker registration, heartbeat monitoring, task dispatching, result acknowledgment, retransmissions, timeout detection, and task reassignment.

The project demonstrates how reliability mechanisms can be implemented at the application layer while using connectionless UDP communication.

### 2. Project Components

The project consists of the following Python files:

- **protocol.py:** Implements the RJDAP_FLEET protocol, including packet construction, packet parsing, message types, CRC32 checksum validation, payload encoding/decoding, and fragmentation/reassembly utilities.

- **server.py:** Implements the Command Hub. It registers workers, maintains the task queue, distributes tasks, monitors worker activity, receives results, acknowledges completed tasks, and reassigns tasks when necessary.

- **worker.py:** Implements the worker nodes. Each worker registers with the server, periodically sends heartbeat messages, receives and computes assigned tasks, and sends results back with acknowledgment and retransmission handling.

- **baseline_test.py:** Automates the baseline test by starting the server and four worker processes, monitoring execution, collecting results, and verifying the acceptance criteria.

### 3. Protocol Design

**Protocol Name:** RJDAP_FLEET  
**Protocol Version:** 1  
**Transport Protocol:** UDP  
**Maximum Payload Size:** 512 bytes  
**Header Size:** 20 bytes

The protocol defines five message types:

1. WORKER_JOIN: Sent by a worker to register with the server.
2. HEARTBEAT: Sent periodically to indicate that a worker is still active.
3. TASK_DISPATCH: Sent by the server to assign a computational task to a worker.
4. TASK_RESULT: Sent by a worker to return its computation result.
5. ACK_RESULT: Sent by the server to acknowledge receipt of a task result.

Each packet contains a fixed-size header followed by an optional payload.

The header includes:
- Protocol version
- Message type
- Worker ID
- Task ID
- Attempt number
- Fragment index
- Total number of fragments
- CRC32 checksum
- Timestamp
- Flags

**Reliability Mechanisms:**

- CRC32 checksum validation to detect corrupted packets.
- Heartbeat messages to monitor worker availability.
- Result acknowledgments to confirm successful receipt.
- Retransmissions with exponential backoff when acknowledgments are not received.
- Maximum retransmission attempts to avoid infinite retries.
- Timeout detection and task reassignment when workers fail to complete their tasks.
- Duplicate result detection to prevent completed tasks from being counted more than once.

### 4. Requirements

Before running the project, ensure that you have:

- Python 3 installed.
- All four Python files in the same directory.
- No additional third-party Python packages are required.

### 5. How to Run the Project Locally

The project can be executed locally on a single computer.

**Step 1: Prepare the project folder**

Place the following files in the same directory:

- protocol.py
- server.py
- worker.py
- baseline_test.py

**Step 2: Open a terminal**

Navigate to the project directory.

**Step 3: Run the automated baseline test**

Execute:

python baseline_test.py

The script automatically:

1. Starts the Command Hub (server.py).
2. Launches four concurrent worker processes.
3. Distributes 1,000 computational tasks among the workers.
4. Waits for the server to complete all tasks.
5. Collects worker outputs and statistics.
6. Verifies the baseline acceptance criteria.
7. Displays the final test result.

No manual network configuration or network impairment tools are required for this baseline test.

### 6. Baseline Test Scenario

The baseline scenario uses the following configuration:

- Topology: 1 Command Hub + 4 concurrent workers.
- Environment: Local execution on one computer.
- Transport: UDP.
- Network impairment: None.
- Total tasks: 1,000.
- Server port: 12000.

**Acceptance Criteria:**

The automated test checks the following conditions:

1. All 1,000 tasks are completed successfully.
2. Zero duplicate results are dropped.
3. The four workers collectively compute exactly 1,000 tasks.

The script also reports the total wall-clock execution time.

A successful execution displays:

TEST PASSED: ALL CHECKED CONDITIONS MET!

If any of the checked conditions are not satisfied, the test displays:

TEST FAILED: CRITERIA NOT SATISFIED

### 7. Generated Log Files

The system generates CSV log files during execution:

- **server_log.csv:** Records server-side packet transmissions and receptions, including message type, worker ID, task ID, and attempt number.

- **worker_1_log.csv:** Records communication events for worker 1.

- **worker_2_log.csv:** Records communication events for worker 2.

- **worker_3_log.csv:** Records communication events for worker 3.

- **worker_4_log.csv:** Records communication events for worker 4.

These files can be used to inspect protocol activity and analyze the execution of the baseline scenario.

### 8. Demonstration Video

The demonstration video presents the project execution, protocol operation, worker registration, task distribution, and baseline test results.

Video Link:

[INSERT YOUR VIDEO LINK HERE]

Note: The video must be uploaded online, and its sharing permissions must be set to "Anyone with the link can view".

### 9. Project Execution Summary

RJDAP_FLEET demonstrates reliable task distribution over UDP using application-layer mechanisms.

The Command Hub coordinates multiple workers, assigns computational tasks, monitors worker availability, and ensures that completed results are acknowledged and duplicate results are handled appropriately.

The baseline test provides an automated way to evaluate the system under normal local network conditions without network impairment.
