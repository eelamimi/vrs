#!/usr/bin/env python3
"""
Клиент для Lab 3.

    python client.py put x 42
    python client.py get x
    python client.py status        # роль/term/данные каждого живого узла
    python client.py --cluster 1:9001,2:9002,3:9003,4:9004,5:9005 put x 42

Клиент не знает, кто лидер: перебирает узлы, следует за redirect
("I am not leader, current leader is X") и повторяет попытки,
пока идёт failover.
"""
import argparse
import json
import socket
import sys
import time

DEFAULT_CLUSTER = "1:9001,2:9002,3:9003"


def parse_cluster(s):
    cluster = {}
    for item in s.split(","):
        nid, port = item.strip().split(":")
        cluster[int(nid)] = ("127.0.0.1", int(port))
    return cluster


def send(addr, msg, timeout=5.0):
    """Один запрос -> один ответ. None, если узел недоступен."""
    try:
        with socket.create_connection(addr, timeout=timeout) as s:
            s.settimeout(timeout)
            s.sendall((json.dumps(msg) + "\n").encode("utf-8"))
            line = s.makefile("r", encoding="utf-8").readline()
        return json.loads(line) if line else None
    except (OSError, ValueError):
        return None


def request(cluster, msg, retries=10, delay=1.0, verbose=True):
    """Найти лидера и выполнить запрос. Retry нужен на время failover (3-8 с)."""
    last = {"type": "ERROR", "error": "no_reply"}
    for attempt in range(1, retries + 1):
        for nid in sorted(cluster, reverse=True):          # сильные узлы - вероятные лидеры
            tried = [nid]
            while tried:
                cur = tried.pop()
                reply = send(cluster[cur], msg)
                if reply is None:
                    continue                               # узел не отвечает
                if reply.get("type") == "ERROR":
                    last = reply
                    leader = reply.get("leader")
                    if reply.get("error") == "not_leader" and leader in cluster and leader != cur:
                        if verbose:
                            print(f"  узел {cur}: не лидер, redirect -> {leader}")
                        tried.append(leader)               # сразу идём к указанному лидеру
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
    ap.add_argument("command", choices=["put", "get", "status"])
    ap.add_argument("args", nargs="*")
    a = ap.parse_args()
    cluster = parse_cluster(a.cluster)

    if a.command == "status":
        for nid in sorted(cluster):
            r = send(cluster[nid], {"type": "DUMP"}, timeout=1.0)
            if r:
                print(f"узел {nid}: {r['state']:9} term={r['term']} leader={r['leader']} data={r['data']}")
            else:
                print(f"узел {nid}: НЕДОСТУПЕН")
        return

    if a.command == "put":
        if len(a.args) != 2:
            sys.exit("usage: client.py put <key> <value>")
        try:
            value = json.loads(a.args[1])        # 42 -> int, "abc" без кавычек -> строка
        except ValueError:
            value = a.args[1]
        msg = {"type": "PUT", "key": a.args[0], "value": value}
    else:
        if len(a.args) != 1:
            sys.exit("usage: client.py get <key>")
        msg = {"type": "GET", "key": a.args[0]}

    print(request(cluster, msg, retries=a.retries))


if __name__ == "__main__":
    main()
