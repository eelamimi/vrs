#!/usr/bin/env python3
"""
Замер времени recovery до/после snapshot на разных длинах истории (без сети, один узел).

    python bench_recovery.py                # N = 1000, 10000, 50000
    python bench_recovery.py 200000

Для каждого N: пишем N записей PUT в лог (пачками, чтобы не ждать N fsync'ов), помечаем их
закоммиченными и меряем создание Node с диска (= recovery) в двух режимах:
  - без snapshot: загрузка лога N записей + повторное применение всех
  - со snapshot:  загрузка snapshot'а + короткого хвоста
"""
import logging
import os
import shutil
import sys
import tempfile
import time

from node import Node
from storage import Storage

logging.disable(logging.CRITICAL)
CL = {1: ("127.0.0.1", 9999)}


def build(n, d):
    s = Storage(d)
    s.load()
    batch = []
    for i in range(1, n + 1):
        batch.append({"term": 1, "index": i, "command": {
            "op": "PUT", "key": f"key{i % 5000}", "value": i, "client_id": f"c{i % 50}", "req_id": i}})
        if len(batch) == 1000:
            s.append_entries(batch)
            batch = []
    s.append_entries(batch)
    s.save_hard_state(1, None)
    s.save_commit(n)
    s.close()


def timed_load(d, threshold):
    t = time.time()
    node = Node(1, CL, d, snapshot_threshold=threshold, metrics_interval=3600)
    return (time.time() - t) * 1000, node


def main():
    sizes = [int(x) for x in sys.argv[1:]] or [1000, 10000, 50000]
    print(f"{'N записей':>10} | {'режим':>12} | {'log_len':>8} | {'размер log':>11} | "
          f"{'snapshot':>9} | {'recovery, мс':>12}")
    print("-" * 78)
    for n in sizes:
        d = tempfile.mkdtemp(prefix="bench_")
        build(n, d)
        ms, node = timed_load(d, 10 ** 9)
        logsz = os.path.getsize(os.path.join(d, "log.jsonl"))
        print(f"{n:>10} | {'без snapshot':>12} | {len(node.log):>8} | {logsz:>9} B | "
              f"{'-':>9} | {ms:>12.1f}")
        # делаем snapshot (порог 1 -> maybe_snapshot срабатывает), затем перезапуск
        node.snapshot_threshold = 1
        node.maybe_snapshot()
        node.storage.close()
        del node
        ms2, node2 = timed_load(d, 10 ** 9)
        logsz2 = os.path.getsize(os.path.join(d, "log.jsonl"))
        snsz = os.path.getsize(os.path.join(d, "snapshot.json"))
        assert node2.last_applied == n and len(node2.state) == min(n, 5000)
        print(f"{n:>10} | {'со snapshot':>12} | {len(node2.log):>8} | {logsz2:>9} B | "
              f"{snsz:>7} B | {ms2:>12.1f}")
        shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    main()
