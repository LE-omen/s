#!/usr/bin/env python3
"""V12 IPC admission/concurrency probe: slot reuse, stale generations, invalid handles.

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
            assert magic == b"NS12" and 0 < size <= 256*1024
            return read_exact(size)

        sequence = 0
        def send(tag, payload):
            frame = payload[:1] + numbers(*tag) + payload[1:]
            proc.stdin.write(b"NS12" + struct.pack("<I", len(frame)) + frame)
            proc.stdin.flush()

        def receive():
            frame = read()
            assert len(frame) >= 17
            return struct.unpack("<QQ", frame[1:17]), frame[:1] + frame[17:]

        def command(payload, expected=0):
            nonlocal sequence
            sequence += 1
            tag = (0, sequence)
            send(tag, payload)
            replies = []
            while True:
                actual, reply = receive()
                assert actual == tag, (actual, tag, base)
                if reply[:1] == b"D":
                    assert len(reply) == 9 and struct.unpack("<q", reply[1:])[0] == expected, (reply, base)
                    return replies
                assert reply[:1] in (b"a", b"S", b"H", b"R"), (reply, base)
                send(tag, b"K")
                replies.append(reply)

        def query_payload(handle, sql):
            text = sql.encode()
            return b"Q" + numbers(*handle, 1, len(text)) + text

        def open_session(sid):
            # Invalid default DB avoids catalog requests. Seven protocol scalars:
            # client/connection/results charset, connection/DB collation, mode, timeout.
            reply, = command(b"A" + numbers(sid, (1<<64)-1, 0, 45, 45, 45, 45, 45, 0, 30000000))
            assert reply[:1] == b"a" and len(reply) == 17
            return struct.unpack("<QQ", reply[1:])

        def query(handle, sql, expected=0):
            return command(query_payload(handle, sql), expected)

        try:
            assert receive() == ((0, 0), b"Y" + numbers(2))
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
            query(reused, "SET @x=17")
            query(handles[0], "SET @x=29")
            fast_row = query(handles[0], "SELECT @x")[-1]
            send((1, 1), query_payload(reused, "SELECT SLEEP(2),@x"))
            send((2, 1), query_payload(handles[0], "SELECT @x"))
            send((3, 1), query_payload(reused, "SELECT 999"))
            done, replies = [], {}
            while len(done) < 3:
                tag, reply = receive()
                assert tag in ((1,1), (2,1), (3,1)), (tag, base)
                if reply[:1] == b"D":
                    expected = -4023 if tag == (3,1) else 0
                    assert struct.unpack("<q", reply[1:])[0] == expected, (tag,reply,base)
                    done.append(tag)
                else:
                    send(tag, b"K")
                    replies.setdefault(tag, []).append(reply)
            assert done.index((2,1)) < done.index((1,1)), done
            assert replies[(2,1)][-1] == fast_row

            # Hold all credits after the first frame. Control and a different
            # session must still progress; an old request generation cannot
            # accidentally grant credit to its replacement.
            send((1,2), query_payload(reused, "SELECT @x"))
            tag, reply = receive()
            assert tag == (1,2) and reply[:1] == b"S", (tag,reply,base)
            send((1,1), b"K")
            assert query(handles[0], "SELECT @x")[-1] == fast_row
            assert not select.select([proc.stdout], [], [], .2)[0], "stale credit released replacement request"
            command(b"C" + numbers(*reused))
            replacement = open_session(201)
            assert replacement == (reused[0], reused[1]+1)
            assert query(replacement, "SELECT @x")[-1] != baseline
            send((1,2), b"K")
            old_rows = []
            while True:
                tag, reply = receive()
                assert tag == (1,2), tag
                if reply[:1] == b"D":
                    assert struct.unpack("<q", reply[1:])[0] == 0, (reply,base)
                    break
                old_rows.append(reply)
                send(tag, b"K")
            assert old_rows[-1] == baseline, "in-flight session was freed/rebound"
            reused = replacement
            for handle in [reused] + handles:
                command(b"C" + numbers(*handle))
            command(b"C" + numbers(*reused), -5066)
            assert "active=0 slots=17" in (base / "process.out").read_text()
            print(f"PASS: concurrent execution, bounded credits, control progress, in-flight close/reuse, stale handles, active=0; {base}")
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
