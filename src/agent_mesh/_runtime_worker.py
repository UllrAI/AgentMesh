"""Own a Runtime process group independently of the daemon."""

import ctypes
import json
import os
import select
import signal
import subprocess
import sys
import time

TERMINATION_GRACE = 1.0
POLL_INTERVAL = 0.05


def write_state(descriptor, phase, **fields):
    # The daemon reads this file only after it can take the worker's lock.
    data = json.dumps({"phase": phase, **fields}).encode()
    os.lseek(descriptor, 0, os.SEEK_SET)
    os.write(descriptor, data)
    os.ftruncate(descriptor, len(data))
    os.fsync(descriptor)


def group_exists(group):
    try:
        os.killpg(group, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        pass  # Darwin can report EPERM for a group containing a zombie.
    return True


def signal_group(group, sig):
    try:
        os.killpg(group, sig)
    except (ProcessLookupError, PermissionError):
        pass


def reap_descendants():
    # Linux reparents orphans to the configured subreaper, even inside a container.
    while True:
        try:
            if os.waitpid(-1, os.WNOHANG)[0] == 0:
                return
        except ChildProcessError:
            return


def clean_group(process):
    group = process.pid
    signal_group(group, signal.SIGTERM)
    deadline = time.monotonic() + TERMINATION_GRACE
    while group_exists(group) and time.monotonic() < deadline:
        if process.poll() is not None:
            reap_descendants()
        time.sleep(POLL_INTERVAL)
    if group_exists(group):
        signal_group(group, signal.SIGKILL)
    while group_exists(group):
        if process.poll() is not None:
            reap_descendants()
        time.sleep(POLL_INTERVAL)
    process.wait()
    reap_descendants()


def supervise(guard, owner, timeout, arguments):
    # On other systems the service's init process reaps orphaned descendants.
    if sys.platform.startswith("linux"):
        ctypes.CDLL(None).prctl(36, 1, 0, 0, 0)  # PR_SET_CHILD_SUBREAPER

    stopping = False

    def stop(_signum, _frame):
        nonlocal stopping
        stopping = True

    def owner_closed(wait=0):
        return stopping or bool(select.select([owner], [], [], wait)[0])

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    if owner_closed():
        write_state(guard, "finished", reason="owner_closed")
        return

    write_state(guard, "active")
    deadline = time.monotonic() + timeout
    try:
        process = subprocess.Popen(arguments, start_new_session=True)
    except OSError:
        write_state(guard, "finished", reason="launch_error")
        return
    reason = "exit"
    try:
        write_state(guard, "active", group=process.pid)
        # Only the Runtime should hold its input open once the spawn succeeds.
        os.close(0)
        while process.poll() is None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                reason = "timeout"
                break
            if owner_closed(min(POLL_INTERVAL, remaining)):
                reason = "owner_closed"
                break
    finally:
        clean_group(process)
    write_state(guard, "finished", reason=reason, returncode=process.returncode)


if __name__ == "__main__":
    supervise(int(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3]), sys.argv[4:])
