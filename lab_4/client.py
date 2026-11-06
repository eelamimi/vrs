# python client.py put x 42
# python client.py put x 42 --op-id last_id
# python client.py get x
# python client.py status
# python client.py crash 3 after_commit

import argparse
import json
import socket
import sys
import time
import uuid

DEFAULT_CLUSTER = "1:9001,2:9002,3:9003"


def parse_cluster(s):
    cluster = {}
    for item in s.split(","):
        nid, port = item.strip().split(":")
        cluster[int(nid)] = ("127.0.0.1", int(port))
    return cluster


def send(addr, msg, timeout=8.0):
    try:
        with socket.create_connection(addr, timeout=timeout) as s:
            s.settimeout(timeout)
            s.sendall((json.dumps(msg) + "\n").encode("utf-8"))
            line = s.makefile("r", encoding="utf-8").readline()
        return json.loads(line) if line else None
    except (OSError, ValueError):
        return None


def request(cluster, msg, retries=10, delay=1.0, verbose=True):
    last = {"type": "ERROR", "error": "no_reply"}
    for attempt in range(1, retries + 1):
        for nid in sorted(cluster, reverse=True):
            tried = [nid]
            seen = set()
            while tried:
                cur = tried.pop()
                if cur in seen:
                    continue
                seen.add(cur)
                reply = send(cluster[cur], msg)
                if reply is None:
                    continue
                if reply.get("type") == "ERROR":
                    last = reply
                    leader = reply.get("leader")
                    if reply.get("error") == "not_leader" and leader in cluster and leader != cur:
                        if verbose:
                            print(f"  узел {cur}: не лидер, redirect -> {leader}")
                        tried.append(leader)
                    continue
                return reply
        if verbose:
            print(f"  попытка {attempt}/{retries} не удалась ({last.get('error')}), жду {delay} с...")
        time.sleep(delay)
    return last


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cluster", default=DEFAULT_CLUSTER)
    ap.add_argument("--retries", type=int, default=10)
    ap.add_argument("--op-id", default=None)
    ap.add_argument("command", choices=["put", "get", "status", "crash"])
    ap.add_argument("args", nargs="*")
    a = ap.parse_args()
    cluster = parse_cluster(a.cluster)

    if a.command == "status":
        for nid in sorted(cluster):
            r = send(cluster[nid], {"type": "DUMP"}, timeout=1.0)
            if r:
                print(f"узел {nid}: {r['state']:9} term={r['term']} leader={r['leader']} "
                      f"log_len={r['log_len']} commit={r['commit_index']} "
                      f"applied={r['last_applied']} data={r['data']}")
            else:
                print(f"узел {nid}: НЕДОСТУПЕН")
        return

    if a.command == "crash":
        if len(a.args) != 2:
            sys.exit("usage: client.py crash <node_id> <after_commit|before_replicate|none>")
        point = None if a.args[1] == "none" else a.args[1]
        print(send(cluster[int(a.args[0])], {"type": "CRASH_POINT", "point": point}))
        return

    if a.command == "put":
        if len(a.args) != 2:
            sys.exit("usage: client.py put <key> <value>")
        try:
            value = json.loads(a.args[1])
        except ValueError:
            value = a.args[1]
        op_id = a.op_id or str(uuid.uuid4())  # один op_id на все retry
        print(f"op_id={op_id}")
        msg = {"type": "PUT", "key": a.args[0], "value": value, "op_id": op_id}
    else:
        if len(a.args) != 1:
            sys.exit("usage: client.py get <key>")
        msg = {"type": "GET", "key": a.args[0]}

    print(request(cluster, msg, retries=a.retries))


if __name__ == "__main__":
    main()
