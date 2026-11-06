#!/usr/bin/env python3
"""
Клиент для Lab 6 (интерфейс как в Lab 5; status показывает snapshot и метрики).

    python client.py put x 42                          # client_id и req_id генерируются/ведутся автоматически
    python client.py put x 42 --client-id alice --req-id 7
    python client.py put x 42 --client-id alice --req-id 7    # ТОТ ЖЕ запрос ещё раз (имитация retry):
                                                              # эффект не удвоится, index тот же
    python client.py get x
    python client.py status [--full]                   # роль/term/log/commit/applied/last_req/метрики диска
    python client.py crash 3 after_commit              # fault injection: after_commit | before_replicate | none

Идентификаторы:
  * client_id - устойчивый идентификатор клиента. По умолчанию хранится в файле .client_id
    рядом с client.py (создаётся при первом запуске), поэтому разные запуски - один клиент.
  * req_id - монотонный счётчик логических запросов; по умолчанию хранится в .req_id_<client_id>
    и увеличивается ПЕРЕД новым put. Внутренние retry (failover, таймауты) идут с ТЕМ ЖЕ req_id.
    Для ручного повтора передайте --req-id явно (счётчик при этом не трогается).
"""
import argparse
import json
import os
import socket
import sys
import time
import uuid

DEFAULT_CLUSTER = "1:9001,2:9002,3:9003"
HERE = os.path.dirname(os.path.abspath(__file__))


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
    """Найти лидера и выполнить запрос; msg (с теми же client_id/req_id) повторяется при failover."""
    last = {"type": "ERROR", "error": "no_reply"}
    for attempt in range(1, retries + 1):
        for nid in sorted(cluster, reverse=True):
            tried, seen = [nid], set()
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


def load_client_id():
    path = os.path.join(HERE, ".client_id")
    try:
        with open(path, encoding="utf-8") as f:
            cid = f.read().strip()
            if cid:
                return cid
    except OSError:
        pass
    cid = "c-" + uuid.uuid4().hex[:8]
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(cid)
    except OSError:
        pass
    return cid


def next_req_id(client_id):
    path = os.path.join(HERE, f".req_id_{client_id}")
    try:
        with open(path, encoding="utf-8") as f:
            n = int(f.read().strip())
    except (OSError, ValueError):
        n = 0
    n += 1
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(str(n))
    except OSError:
        pass
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cluster", default=DEFAULT_CLUSTER)
    ap.add_argument("--retries", type=int, default=10)
    ap.add_argument("--client-id", default=None)
    ap.add_argument("--req-id", type=int, default=None)
    ap.add_argument("--full", action="store_true", help="status: показать весь лог каждого узла")
    ap.add_argument("command", choices=["put", "get", "status", "crash"])
    ap.add_argument("args", nargs="*")
    a = ap.parse_args()
    cluster = parse_cluster(a.cluster)

    if a.command == "status":
        for nid in sorted(cluster):
            r = send(cluster[nid], {"type": "DUMP", "full": a.full}, timeout=1.0)
            if r:
                print(f"узел {nid}: {r['state']:9} term={r['term']} leader={r['leader']} "
                      f"log_len={r['log_len']} last_idx={r['last_log_index']} snap_idx={r['snap_idx']} "
                      f"commit={r['commit_index']} applied={r['last_applied']} "
                      f"snapshots={r['snapshots_taken']}+{r['snapshots_installed']}inst "
                      f"recovery={r['recovery_ms']}мс keys={len(r['data'])}")
                if not a.full:
                    print(f"         data={r['data']} last_req={r['last_req']}")
                if a.full:
                    for e in r["log"]:
                        print(f"     idx={e['index']} term={e['term']} {e['op']} "
                              f"{e['key'] or ''} {e['client_id'] or ''} {e['req_id'] or ''}")
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
        cid = a.client_id or load_client_id()
        rid = a.req_id if a.req_id is not None else next_req_id(cid)
        print(f"client_id={cid} req_id={rid}")
        msg = {"type": "PUT", "key": a.args[0], "value": value, "client_id": cid, "req_id": rid}
    else:
        if len(a.args) != 1:
            sys.exit("usage: client.py get <key>")
        msg = {"type": "GET", "key": a.args[0]}

    print(request(cluster, msg, retries=a.retries))


if __name__ == "__main__":
    main()
