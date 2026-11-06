#!/usr/bin/env python3
"""
Автоматический прогон сценариев шага 6 (Lab 6) на кластере из 3 процессов, с kill -9.

    python test_scenarios.py            # все сценарии по порядку (они накапливают данные)
    python test_scenarios.py 1 2 3      # префикс цепочки

Сценарии:
  1. Рост лога без snapshot (THRESHOLD огромный): 40 PUT, log_len и время рестарта follower
  2. Snapshot сработал (THRESHOLD=20): в логах SNAPSHOT, log_len упал, state на всех узлах совпал
  3. Restart после snapshot: kill -9 follower, snap=N и короткий лог, GET актуален, время рестарта
  4. Failover после snapshot: убит лидер, новый лидер обслуживает GET/PUT
  5. Идемпотентность после snapshot + рестарта всех узлов
  6. (бонус) InstallSnapshot: follower отстал за границу snapshot'а лидера и догоняется snapshot'ом

Создаёт чистый run/ (data/node*/ и logs/node*.log); в конце печатает таблицу "до/после snapshot".
"""
import os
import shutil
import signal
import subprocess
import sys
import time

import client as cl

HERE = os.path.dirname(os.path.abspath(__file__))
RUN = os.path.join(HERE, "run")
CLUSTER = cl.parse_cluster("1:9001,2:9002,3:9003")
NO_SNAPSHOT = 10 ** 9
THRESHOLD = 20

expected = {}          # накопленное ожидаемое состояние KV
rid_counter = {"w": 0}
table = []             # строки итоговой таблицы recovery


class Cluster:
    def __init__(self):
        shutil.rmtree(RUN, ignore_errors=True)
        os.makedirs(os.path.join(RUN, "logs"))
        self.procs = {}

    def start(self, i, threshold):
        log = open(os.path.join(RUN, "logs", f"node{i}.log"), "ab")
        t0 = time.time()
        self.procs[i] = subprocess.Popen(
            [sys.executable, "-u", os.path.join(HERE, "node.py"), str(i),
             "--data-dir", os.path.join(RUN, "data"),
             "--snapshot-threshold", str(threshold), "--metrics-interval", "2"],
            stdout=log, stderr=subprocess.STDOUT, cwd=HERE)
        return t0

    def start_all(self, threshold):
        for i in CLUSTER:
            self.start(i, threshold)

    def kill9(self, i):
        p = self.procs.pop(i, None)
        if p and p.poll() is None:
            p.send_signal(signal.SIGKILL)
            p.wait()
        print(f"    kill -9 узел {i}")

    def kill_all(self):
        for i in list(self.procs):
            self.kill9(i)

    def start_timed(self, i, threshold):
        """Старт узла и время до первого ответа на запрос (= 'готов принимать запросы')."""
        t0 = self.start(i, threshold)
        d = wait_for(lambda: dump(i), timeout=15, what=f"старт узла {i}")
        return d, round((time.time() - t0) * 1000)


def dump(i, full=False):
    return cl.send(CLUSTER[i], {"type": "DUMP", "full": full}, timeout=1.0)


def leader():
    best = None
    for i in CLUSTER:
        r = dump(i)
        if r and r["state"] == "leader" and (best is None or r["term"] > best[1]):
            best = (i, r["term"])
    return best[0] if best else None


def wait_for(cond, timeout=30, what=""):
    t0 = time.time()
    while time.time() - t0 < timeout:
        v = cond()
        if v:
            return v
        time.sleep(0.25)
    raise AssertionError(f"таймаут ожидания: {what}")


def wait_leader():
    return wait_for(leader, what="лидер")


def put(key, value, cid="w", rid=None):
    if cid == "w":
        rid_counter["w"] += 1
        rid = rid_counter["w"]
    msg = {"type": "PUT", "key": key, "value": value, "client_id": cid, "req_id": rid}
    ld = leader()
    if ld:
        r = cl.send(CLUSTER[ld], msg)
        if r and r.get("type") == "PUT_OK":
            expected[key] = value
            return r
    r = cl.request(CLUSTER, msg, retries=20, verbose=False)
    if r.get("type") == "PUT_OK":
        expected[key] = value
    return r


def get(key):
    return cl.request(CLUSTER, {"type": "GET", "key": key}, retries=20, verbose=False)


def converged(nodes=None):
    nodes = nodes or list(CLUSTER)
    ds = [dump(i) for i in nodes]
    if any(d is None for d in ds):
        return False
    c = max(d["commit_index"] for d in ds)
    return all(d["data"] == expected and d["commit_index"] == c and d["last_applied"] == c
               for d in ds)


def fill(n, prefix):
    for _ in range(n):
        k = f"{prefix}{rid_counter['w'] + 1}"
        r = put(k, rid_counter["w"] + 1)
        assert r["type"] == "PUT_OK", r


def grep_logs(pattern, node=None):
    out = []
    for i in ([node] if node else sorted(CLUSTER)):
        p = os.path.join(RUN, "logs", f"node{i}.log")
        for line in open(p, encoding="utf-8", errors="replace"):
            if pattern in line:
                out.append((i, line.strip()))
    return out


def show_files():
    for i in sorted(CLUSTER):
        d = os.path.join(RUN, "data", f"node{i}")
        if os.path.isdir(d):
            files = ", ".join(f"{f}({os.path.getsize(os.path.join(d, f))}B)"
                              for f in sorted(os.listdir(d)))
            print(f"    data/node{i}/: {files}")


def any_follower():
    ld = wait_leader()
    return [i for i in CLUSTER if i != ld][0]


# ------------------------------------------------------------------ сценарии
def s1(c):
    """Рост лога без snapshot."""
    c.start_all(NO_SNAPSHOT)
    ld = wait_leader()
    fill(40, "k")
    wait_for(converged, what="сходимость")
    fol = any_follower()
    before = dump(fol)
    print(f"    без snapshot: узел {fol} log_len={before['log_len']} snap_idx={before['snap_idx']} "
          f"last_idx={before['last_log_index']}")
    assert before["snap_idx"] == 0 and before["log_len"] == before["last_log_index"] >= 41
    c.kill9(fol)
    d, ready = c.start_timed(fol, NO_SNAPSHOT)
    print(f"    рестарт без snapshot: log_len={d['log_len']} recovery={d['recovery_ms']} мс, "
          f"готов через {ready} мс (внутр. {d['ready_ms']} мс)")
    assert d["log_len"] == before["log_len"] and d["data"] == expected
    table.append(("без snapshot", d["log_len"], d["snap_idx"], d["recovery_ms"], ready))
    wait_for(converged, what="сходимость после рестарта")


def s2(c):
    """Snapshot сработал: перезапуск кластера с THRESHOLD=20."""
    c.kill_all()
    time.sleep(0.5)
    c.start_all(THRESHOLD)
    wait_leader()
    fill(30, "m")
    wait_for(converged, what="сходимость")
    for i in CLUSTER:
        d = dump(i)
        print(f"    узел {i}: snap_idx={d['snap_idx']} log_len={d['log_len']} "
              f"last_idx={d['last_log_index']} snapshots={d['snapshots_taken']}")
        assert d["snap_idx"] > 0 and d["snapshots_taken"] >= 1, f"узел {i}: snapshot не создан"
        assert d["log_len"] < THRESHOLD, f"узел {i}: лог не укоротился ({d['log_len']})"
        assert d["data"] == expected
    snaps = grep_logs("*** SNAPSHOT ***")
    assert snaps
    print(f"    строк SNAPSHOT в логах: {len(snaps)}, например: {snaps[0][1][-110:]}")
    show_files()


def s3(c):
    """Restart после snapshot."""
    fol = any_follower()
    before = dump(fol)
    c.kill9(fol)
    d, ready = c.start_timed(fol, THRESHOLD)
    print(f"    после рестарта: snap_idx={d['snap_idx']} log_len={d['log_len']} "
          f"last_idx={d['last_log_index']} (история {d['last_log_index']} записей) "
          f"recovery={d['recovery_ms']} мс, готов через {ready} мс")
    assert d["snap_idx"] > 0 and d["snap_idx"] >= before["snap_idx"] - 0
    assert d["log_len"] < 41, "лог после рестарта должен быть короче истории"
    assert d["data"] == {k: v for k, v in expected.items() if k in d["data"]}
    table.append(("со snapshot", d["log_len"], d["snap_idx"], d["recovery_ms"], ready))
    wait_for(converged, what="сходимость")
    for k in ("k1", "k40", "m70"):
        if k in expected:
            r = get(k)
            assert r["type"] == "GET_OK" and r["value"] == expected[k], (k, r)
    ms = grep_logs("METRICS", fol)[-1][1]
    print(f"    METRICS: {ms[ms.index('METRICS'):][:170]}")


def s4(c):
    """Failover после snapshot."""
    ld = wait_leader()
    c.kill9(ld)
    rest = [i for i in CLUSTER if i != ld]
    new = wait_for(lambda: (lambda l: l if l in rest else None)(leader()), what="новый лидер")
    print(f"    новый лидер: {new}")
    for k in ("k5", "k33", "m60"):
        if k in expected:
            r = get(k)
            assert r["type"] == "GET_OK" and r["value"] == expected[k], (k, r)
    fill(5, "f")
    c.start(ld, THRESHOLD)
    wait_for(converged, what="сходимость после возврата старого лидера")
    assert dump(ld)["state"] == "follower"
    d = dump(ld)
    print(f"    старый лидер {ld} вернулся: snap_idx={d['snap_idx']} log_len={d['log_len']} "
          f"ключей={len(d['data'])}")


def s5(c):
    """Идемпотентность после snapshot + рестарт всех узлов."""
    first = put("ix", 1, cid="idem", rid=1)
    assert first["type"] == "PUT_OK" and first["duplicate"] is False
    fill(40, "g")
    wait_for(converged, what="сходимость")
    d = dump(wait_leader())
    assert d["snap_idx"] > first["index"], "запись (idem,1) должна быть внутри snapshot'а"
    print(f"    (idem,1) на idx={first['index']}, snapshot лидера покрывает до {d['snap_idx']}")
    c.kill_all()
    time.sleep(0.5)
    c.start_all(THRESHOLD)
    wait_leader()
    again = cl.request(CLUSTER, {"type": "PUT", "key": "ix", "value": 777,
                                 "client_id": "idem", "req_id": 1}, retries=20, verbose=False)
    print(f"    повтор (idem,1) после snapshot+рестарта: {again}")
    assert again["type"] == "PUT_OK" and again["duplicate"] is True and again["index"] == first["index"]
    assert get("ix")["value"] == 1, "эффект удвоился"
    wait_for(converged, what="сходимость")


def s6(c):
    """InstallSnapshot: follower отстал за границу snapshot'а лидера."""
    ld = wait_leader()
    fol = [i for i in CLUSTER if i != ld][0]
    last_before = dump(fol)["last_log_index"]
    c.kill9(fol)
    fill(60, "h")                                             # лидер сожмёт лог далеко за last_before
    dl = dump(wait_leader())
    print(f"    лидер: snap_idx={dl['snap_idx']}, а у отключённого узла {fol} last_idx={last_before}")
    assert dl["snap_idx"] > last_before
    d, ready = c.start_timed(fol, THRESHOLD)
    wait_for(converged, what="догон через InstallSnapshot")
    d = dump(fol)
    print(f"    узел {fol} после догона: snapshots_installed={d['snapshots_installed']} "
          f"snap_idx={d['snap_idx']} log_len={d['log_len']} ключей={len(d['data'])}")
    assert d["snapshots_installed"] >= 1
    assert grep_logs("INSTALL SNAPSHOT", fol)
    print(f"    {grep_logs('отправляю INSTALL_SNAPSHOT')[0][1][-110:]}")
    show_files()


SCENARIOS = {1: ("Рост лога без snapshot", s1), 2: ("Snapshot сработал", s2),
             3: ("Restart после snapshot", s3), 4: ("Failover после snapshot", s4),
             5: ("Идемпотентность после snapshot+restart", s5),
             6: ("InstallSnapshot отстающему follower'у", s6)}

if __name__ == "__main__":
    chosen = [int(x) for x in sys.argv[1:]] or sorted(SCENARIOS)
    c = Cluster()
    failed = []
    try:
        for n in chosen:
            name, fn = SCENARIOS[n]
            print(f"\n=== Сценарий {n}: {name}", flush=True)
            try:
                fn(c)
                print("  -> PASS", flush=True)
            except AssertionError as e:
                print(f"  -> FAIL: {e}", flush=True)
                failed.append(n)
    finally:
        c.kill_all()
    if table:
        print("\nRecovery follower'а, до/после snapshot:")
        print(f"  {'режим':14} {'log_len':>8} {'snap_idx':>9} {'recovery, мс':>13} {'готов, мс':>10}")
        for row in table:
            print(f"  {row[0]:14} {row[1]:>8} {row[2]:>9} {row[3]:>13} {row[4]:>10}")
    print("\nИтог:", "все сценарии пройдены" if not failed else f"провалены: {failed}")
    sys.exit(1 if failed else 0)
