#!/usr/bin/env python3
"""V13 IPC deadline/cancellation probe: slot reuse, stale generations, invalid handles.

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
            assert magic == b"NS13" and 0 < size <= 256*1024
            return read_exact(size)

        sequence = 0
        def send(tag, payload):
            frame = payload[:1] + numbers(*tag) + payload[1:]
            proc.stdin.write(b"NS13" + struct.pack("<I", len(frame)) + frame)
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
                    assert len(reply) >= 9 and struct.unpack("<q", reply[1:9])[0] == expected, (reply, base)
                    return replies
                assert reply[:1] in (b"a", b"H", b"R", b"o", b"e"), (reply, base)
                send(tag, b"K")
                replies.append(reply)

        def query_payload(handle, sql, timeout=30):
            text = sql.encode()
            return b"Q" + numbers(*handle, time.time_ns()//1000 + int(timeout*1000000), len(text)) + text

        def open_session(sid):
            # Invalid default DB avoids catalog requests. Eight protocol scalars:
            # client/connection/results charset, connection/DB collation, mode, timeout, autocommit.
            reply, = command(b"A" + numbers(sid, 0, (1<<64)-1, 0, 45, 45, 45, 45, 45, 0, 30000000, 1))
            assert reply[:1] == b"a" and len(reply) == 17
            return struct.unpack("<QQ", reply[1:])

        def query(handle, sql, expected=0):
            return [reply for reply in command(query_payload(handle, sql), expected) if reply[:1] == b"R"]

        try:
            send((0, 0), b"B" + numbers(1, 9) + b"127.0.0.1")
            assert receive() == ((0, 0), b"Y" + numbers(2))
            first = open_session(100)
            query(first, "SET @x=17")
            baseline = query(first, "SELECT @x")[-1]
            query(first, "SET @x=999; SELECT @x", -4007)
            handles = [open_session(101+i) for i in range(64)]
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
                    assert struct.unpack("<q", reply[1:9])[0] == expected, (tag,reply,base)
                    done.append(tag)
                else:
                    send(tag, b"K")
                    replies.setdefault(tag, []).append(reply)
            assert done.index((2,1)) < done.index((1,1)), done
            assert [reply for reply in replies[(2,1)] if reply[:1] == b"R"][-1] == fast_row

            # Hold all credits after the first frame. Control and a different
            # session must still progress; an old request generation cannot
            # accidentally grant credit to its replacement.
            send((1,2), query_payload(reused, "SELECT @x"))
            tag, reply = receive()
            assert tag == (1,2) and reply[:1] == b"H", (tag,reply,base)
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
                    assert struct.unpack("<q", reply[1:9])[0] == 0, (reply,base)
                    break
                old_rows.append(reply)
                send(tag, b"K")
            assert [reply for reply in old_rows if reply[:1] == b"R"][-1] == baseline, "in-flight session was freed/rebound"
            reused = replacement
            query(reused, "SET @x=17")

            def finish_cancel(tag, reason=-4012, rpc_reply=None):
                started = time.monotonic()
                send(tag, b"Z" + numbers((1<<64)+reason))
                if rpc_reply is not None:
                    send(tag, rpc_reply)
                while True:
                    actual, reply = receive()
                    assert actual == tag, (actual, tag, base)
                    if reply[:1] == b"D":
                        assert struct.unpack("<q", reply[1:9])[0] == reason, (reply,base)
                        assert time.monotonic()-started < 2, "cancel did not interrupt execution"
                        return
                    send(tag, b"K")

            # Cancel at a native expression checkpoint, without waiting for the
            # ten-second SLEEP or the query's thirty-second deadline.
            send((4,1), query_payload(reused, "SELECT SLEEP(10),@x"))
            # Native driver fetches the first row before exposing metadata.
            time.sleep(.2)
            finish_cancel((4,1), -5065)
            assert query(reused, "SELECT @x")[-1] == baseline
            command(query_payload(reused, "SET @expired=999", timeout=-1), -4012)
            assert query(reused, "SELECT @expired")[-1] != query(reused, "SELECT 999")[-1]

            # Same slot, new generation: stale cancel/credit must have no effect.
            send((4,2), query_payload(reused, "SELECT @x"))
            assert receive()[0] == (4,2)  # withhold the first frame's credit
            send((4,1), b"Z" + numbers((1<<64)-4012))
            send((4,1), b"K")
            assert query(handles[0], "SELECT @x")[-1] == fast_row
            assert not select.select([proc.stdout], [], [], .2)[0]
            finish_cancel((4,2))  # wakes the credit wait; D needs no credit

            # A sent RPC is drained before cleanup. The gateway rejects the
            # outstanding catalog request with the cancellation error.
            send((4,3), query_payload(reused, "USE missing_db"))
            tag, reply = receive()
            assert tag == (4,3) and reply[:1] in (b"d",b"b",b"t",b"i"), (tag,reply,base)
            send(tag, b"K")
            finish_cancel(tag, rpc_reply=b"c" + numbers((1<<64)-4012))

            # Fill both execution threads; cancellation of the queued third
            # query must complete before either running SQL finishes.
            for tag, handle in (((5,1),reused), ((6,1),handles[0])):
                send(tag, query_payload(handle, "SELECT SLEEP(10)"))
            time.sleep(.2)  # both native first-row fetches are sleeping
            send((7,1), query_payload(handles[1], "SET @queued=999", timeout=.2))
            time.sleep(.3)
            finish_cancel((7,1))
            finish_cancel((5,1))
            finish_cancel((6,1))
            assert query(handles[1], "SELECT @queued")[-1] != query(handles[1], "SELECT 999")[-1]
            assert query(reused, "SELECT @x")[-1] == baseline
            for generation in range(4,14):
                send((4,generation), query_payload(reused, "SELECT @x"))
                tag, reply = receive()
                assert tag == (4,generation)
                finish_cancel(tag)
                # A completed cancel remains harmless even after a session's
                # next query has started on another request slot.
                send(tag, b"Z" + numbers((1<<64)-4012))
                assert query(reused, "SELECT @x")[-1] == baseline
            for handle in [reused] + handles:
                command(b"C" + numbers(*handle))
            command(b"C" + numbers(*reused), -5066)
            assert "active=0 slots=65" in (base / "process.out").read_text()
            print(f"PASS: 64-session execution/credit/RPC/queued cancellation, stale cancellation, repeated reuse, concurrency, active=0; {base}")
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
