#!/usr/bin/env python3
"""V11 IPC admission probe: slot reuse, stale generations, invalid handles.

python3 tools/obtest/namespace_worker_handles_prototype.py --binary build_release/src/observer/seekdb
No engine/table reads: complements the real MySQL integration probe.
"""
import argparse
import os
from pathlib import Path
import resource
import select
import struct
import subprocess
import tempfile
import time


def numbers(*values):
    return struct.pack("<" + "Q" * len(values), *values)


def run(binary):
    base = Path(tempfile.mkdtemp(prefix="namespace_handles_PROTOTYPE_"))
    with (base / "process.out").open("wb") as log:
        proc = subprocess.Popen([str(Path(binary).resolve()), "--namespace-sql-worker-prototype", "@2"],
                                cwd=base, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log)
        def read_exact(size):
            data = b""
            deadline = time.monotonic() + 10
            while len(data) < size:
                assert select.select([proc.stdout], [], [], max(0, deadline-time.monotonic()))[0], base
                part = os.read(proc.stdout.fileno(), size-len(data))
                assert part, (proc.poll(), base)
                data += part
            return data

        def read():
            magic, size = struct.unpack("<4sI", read_exact(8))
            assert magic == b"NS10" and 0 < size <= 256*1024
            return read_exact(size)

        def command(payload, expected=0):
            proc.stdin.write(b"NS10" + struct.pack("<I", len(payload)) + payload)
            proc.stdin.flush()
            replies = []
            while True:
                reply = read()
                if reply[:1] == b"D":
                    assert len(reply) == 9 and struct.unpack("<q", reply[1:])[0] == expected, (reply, base)
                    return replies
                assert reply[:1] in (b"a", b"S", b"H", b"R"), (reply, base)
                replies.append(reply)

        def open_session(sid):
            # Invalid default DB avoids catalog requests. Seven protocol scalars:
            # client/connection/results charset, connection/DB collation, mode, timeout.
            reply, = command(b"A" + numbers(sid, (1<<64)-1, 0, 45, 45, 45, 45, 45, 0, 30000000))
            assert reply[:1] == b"a" and len(reply) == 17
            return struct.unpack("<QQ", reply[1:])

        def query(handle, sql, expected=0):
            text = sql.encode()
            return command(b"Q" + numbers(*handle, 1, len(text)) + text, expected)

        try:
            assert read() == b"Y" + numbers(2)
            first = open_session(100)
            query(first, "SET @x=17")
            baseline = query(first, "SELECT @x")[-1]
            query(first, "SET @x=999; SELECT @x", -4007)
            handles = [open_session(101+i) for i in range(16)]
            assert query(first, "SELECT @x")[-1] == baseline
            command(b"C" + numbers(*first))
            reused = open_session(200)
            assert reused == (first[0], first[1]+1), (first, reused)
            assert query(reused, "SELECT @x")[-1] != baseline
            for invalid in (first, (1<<63, 1), (reused[0], 0)):
                query(invalid, "SELECT 1", -5066)
                command(b"C" + numbers(*invalid), -5066)
            query(reused, "SELECT 1")
            for handle in [reused] + handles:
                command(b"C" + numbers(*handle))
            command(b"C" + numbers(*reused), -5066)
            assert "active=0 slots=17" in (base / "process.out").read_text()
            print(f"PASS: growth, reuse, stale/invalid handles, duplicate close, active=0; {base}")
        finally:
            proc.stdin.close()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)
            proc.stdout.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    args = parser.parse_args()
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    run(args.binary)
