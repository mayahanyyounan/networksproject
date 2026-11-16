
"""
test_baseline.py
Automated Baseline Test Scenario for Project 2: RJDAP

Scenario:
- Baseline LAN (no network impairment)
- 1 Command Hub server
- 4 concurrent workers
- 1,000 tasks
"""

import os
import subprocess
import sys
import time

SERVER_IP = "127.0.0.1"
SERVER_PORT = 12000
TOTAL_TASKS = 1000
NUM_WORKERS = 4

SERVER_TIMEOUT = 30
WORKER_EXIT_TIMEOUT = 5


def banner(msg):
    print("\n" + "=" * 65)
    print(f" {msg}")
    print("=" * 65)


def stop_process(process):
    """Stop a process if it is still running."""
    if process.poll() is None:
        process.kill()


def run_baseline():

    server = None
    workers = []

    try:
        banner("RJDAP BASELINE (LAN) AUTOMATED TEST")

        print("Configuration:")
        print(f"  * Protocol        : RJDAP-FLEET over UDP")
        print(f"  * Server Address  : {SERVER_IP}:{SERVER_PORT}")
        print(f"  * Task Queue      : {TOTAL_TASKS} tasks")
        print(f"  * Worker Pool     : {NUM_WORKERS} concurrent nodes")
        print(f"  * Netem Profile   : None (Baseline LAN)")
        print("-" * 65)

        # ----------------------------------------------------
        # 1. Start Server
        # ----------------------------------------------------

        print("\n[Step 1/4] Starting Command Hub (server.py)...")

        server = subprocess.Popen(
            [sys.executable, "-u", "server.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        time.sleep(0.4)

        if server.poll() is not None:
            print("[ERROR] Server failed to start!")

            srv_out, srv_err = server.communicate()

            print("\nServer output:")
            print(srv_out)

            print("\nServer errors:")
            print(srv_err)

            return 1

        # ----------------------------------------------------
        # 2. Start Workers
        # ----------------------------------------------------

        print(
            f"[Step 2/4] Spawning {NUM_WORKERS} compute workers..."
        )

        for wid in range(1, NUM_WORKERS + 1):

            worker = subprocess.Popen(
                [
                    sys.executable,
                    "-u",
                    "worker.py",
                    SERVER_IP,
                    str(SERVER_PORT),
                    str(wid),
                    "2.0",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            workers.append((wid, worker))

            print(
                f"  -> Worker {wid} started "
                f"(PID: {worker.pid})"
            )

            time.sleep(0.05)

        # ----------------------------------------------------
        # 3. Wait for Server Completion
        # ----------------------------------------------------

        print(
            "\n[Step 3/4] Dispatching and computing "
            "1,000 tasks across workers..."
        )

        start_time = time.time()

        try:
            srv_out, srv_err = server.communicate(
                timeout=SERVER_TIMEOUT
            )

        except subprocess.TimeoutExpired:

            print(
                f"\n[ERROR] Test timed out after "
                f"{SERVER_TIMEOUT} seconds!"
            )

            # Stop server and workers
            stop_process(server)

            for _, worker in workers:
                stop_process(worker)

            # Collect server output
            srv_out, srv_err = server.communicate()

            print("\n========== SERVER OUTPUT ==========")
            print(srv_out)

            print("\n========== SERVER ERRORS ==========")
            print(srv_err)

            # Collect worker output
            for wid, worker in workers:

                out, err = worker.communicate()

                print(
                    f"\n========== WORKER {wid} OUTPUT =========="
                )
                print(out)

                print(
                    f"\n========== WORKER {wid} ERRORS =========="
                )
                print(err)

            return 1

        elapsed = time.time() - start_time

        # ----------------------------------------------------
        # 4. Collect Worker Results
        # ----------------------------------------------------

        worker_outputs = {}
        worker_errors = {}

        for wid, worker in workers:

            try:
                wo, we = worker.communicate(
                    timeout=WORKER_EXIT_TIMEOUT
                )

            except subprocess.TimeoutExpired:

                print(
                    f"[WARNING] Worker {wid} did not exit "
                    "within the expected time. Stopping it."
                )

                worker.kill()
                wo, we = worker.communicate()

            worker_outputs[wid] = wo
            worker_errors[wid] = we

        # ----------------------------------------------------
        # Display Server Results
        # ----------------------------------------------------

        banner("SERVER RESULTS")

        print(srv_out.strip())

        if srv_err.strip():
            print("\nServer warnings/errors:")
            print(srv_err.strip())

        # ----------------------------------------------------
        # Display Worker Results
        # ----------------------------------------------------

        banner("WORKERS SUMMARY")

        total_computed = 0

        for wid in range(1, NUM_WORKERS + 1):

            out = worker_outputs.get(wid, "")
            err = worker_errors.get(wid, "")

            print(f"\n--- Worker {wid} ---")
            print(out.strip())

            if err.strip():
                print("\nWorker errors:")
                print(err.strip())

            # Read each worker's computation count
            for line in out.splitlines():

                if "tasks computed" in line.lower():

                    try:
                        count = int(line.split(":")[-1].strip())
                        total_computed += count

                    except ValueError:
                        print(
                            f"[WARNING] Could not read "
                            f"Worker {wid}'s task count."
                        )

        # ----------------------------------------------------
        # Verify Acceptance Criteria
        # ----------------------------------------------------

        banner("ACCEPTANCE CRITERIA VERIFICATION")

        # Check 1: Server completed all tasks
        completed_ok = (
            "All 1000 tasks completed" in srv_out
        )

        # Check 2: Zero duplicate results dropped
        duplicates_ok = (
            "duplicates dropped: 0" in srv_out
        )

        # Check 3: Workers collectively computed 1,000 tasks
        distribution_ok = (
            total_computed == TOTAL_TASKS
        )

        print(
            f"1. 1,000 Tasks Completed   : "
            f"{'PASS [OK]' if completed_ok else 'FAIL [X]'}"
        )

        print(
            f"2. Zero Duplicate Results  : "
            f"{'PASS [OK]' if duplicates_ok else 'FAIL [X]'}"
        )

        print(
            f"3. Tasks Computed          : "
            f"{total_computed}/{TOTAL_TASKS} "
            f"{'PASS [OK]' if distribution_ok else 'FAIL [X]'}"
        )

        print(
            f"4. Total Wall-Clock Time   : "
            f"{elapsed:.3f} seconds"
        )

        print("-" * 65)

        # ----------------------------------------------------
        # Final Result
        # ----------------------------------------------------

        if completed_ok and duplicates_ok and distribution_ok:

            print(
                ">>> TEST PASSED: ALL CHECKED CONDITIONS MET! <<<"
            )

            print("\nGenerated CSV Logs:")

            if os.path.exists("server_log.csv"):
                print(
                    f"  - server_log.csv "
                    f"({os.path.getsize('server_log.csv')} bytes)"
                )

            for wid in range(1, NUM_WORKERS + 1):

                filename = f"worker_{wid}_log.csv"

                if os.path.exists(filename):
                    print(
                        f"  - {filename} "
                        f"({os.path.getsize(filename)} bytes)"
                    )

            return 0

        else:

            print(
                ">>> TEST FAILED: CRITERIA NOT SATISFIED <<<"
            )

            return 1

    finally:

        # Make sure no child process remains running
        if server is not None:
            stop_process(server)

        for _, worker in workers:
            stop_process(worker)


if __name__ == "__main__":
    sys.exit(run_baseline())
