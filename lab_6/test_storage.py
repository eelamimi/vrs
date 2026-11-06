#!/usr/bin/env python3
"""Юнит-тесты Storage (без сети; Lab 5 + snapshot/сжатие Lab 6):  python test_storage.py"""
import os
import shutil
import tempfile

from storage import Storage, StorageError


def entry(i, term=1, key="k"):
    return {"term": term, "index": i, "command": {"op": "PUT", "key": key, "value": i,
                                                  "client_id": "c", "req_id": i}}


def fresh():
    return tempfile.mkdtemp(prefix="stor_")


def test_empty_dir():
    d = fresh()
    st = Storage(d).load()
    assert st == {"current_term": 0, "voted_for": None, "commit_index": 0, "log": [], "snapshot": None}, st


def test_roundtrip():
    d = fresh()
    s = Storage(d)
    s.load()
    s.save_hard_state(5, 2)
    s.append_entries([entry(1), entry(2)])
    s.append_entries([entry(3, term=2)])
    s.save_commit(2)
    s.close()
    st = Storage(d).load()
    assert st["current_term"] == 5 and st["voted_for"] == 2
    assert [e["index"] for e in st["log"]] == [1, 2, 3]
    assert st["log"][2]["term"] == 2
    assert st["commit_index"] == 2


def test_voted_for_none():
    d = fresh()
    s = Storage(d)
    s.load()
    s.save_hard_state(3, None)
    assert Storage(d).load()["voted_for"] is None


def test_truncate_then_append():
    d = fresh()
    s = Storage(d)
    s.load()
    s.append_entries([entry(i) for i in range(1, 6)])
    s.truncate_from(3)
    s.append_entries([entry(3, term=2), entry(4, term=2)])
    s.close()
    log = Storage(d).load()["log"]
    assert [(e["index"], e["term"]) for e in log] == [(1, 1), (2, 1), (3, 2), (4, 2)], log


def test_torn_tail_is_dropped():
    """kill -9 посреди записи: последняя строка оборвана (нет '\\n')."""
    d = fresh()
    s = Storage(d)
    s.load()
    s.append_entries([entry(1), entry(2)])
    s.close()
    with open(os.path.join(d, "log.jsonl"), "ab") as f:
        f.write(b'0badc0de\t{"term":1,"index":3,"comm')          # оборванная запись
    s2 = Storage(d)
    log = s2.load()["log"]
    assert [e["index"] for e in log] == [1, 2]
    assert s2.stats["dropped_bytes"] > 0
    s2.append_entries([entry(3)])                                 # после восстановления можно писать дальше
    s2.close()
    assert [e["index"] for e in Storage(d).load()["log"]] == [1, 2, 3]


def test_corrupt_crc_truncates_from_damage():
    d = fresh()
    s = Storage(d)
    s.load()
    s.append_entries([entry(i) for i in range(1, 5)])
    off = s.offsets[1]                                            # начало записи 2
    s.close()
    p = os.path.join(d, "log.jsonl")
    data = bytearray(open(p, "rb").read())
    data[off + 12] ^= 0xFF                                        # портим байт внутри записи 2
    open(p, "wb").write(bytes(data))
    log = Storage(d).load()["log"]
    assert [e["index"] for e in log] == [1], log                  # 2,3,4 недостоверны


def test_corrupt_meta_gives_defaults():
    d = fresh()
    s = Storage(d)
    s.load()
    s.append_entries([entry(1)])
    s.close()
    open(os.path.join(d, "meta.json"), "w").write("{not json")
    s2 = Storage(d)
    st = s2.load()
    assert st["current_term"] == 0 and st["voted_for"] is None
    assert s2.stats["meta_corrupt"] and len(st["log"]) == 1


def test_commit_clamped_to_log_len():
    d = fresh()
    s = Storage(d)
    s.load()
    s.append_entries([entry(1)])
    s.save_commit(99)
    s.close()
    assert Storage(d).load()["commit_index"] == 1


def test_save_log_full_rewrite():
    d = fresh()
    s = Storage(d)
    s.load()
    s.append_entries([entry(1)])
    s.save_log([entry(1), entry(2), entry(3)])
    s.append_entries([entry(4)])
    s.close()
    assert [e["index"] for e in Storage(d).load()["log"]] == [1, 2, 3, 4]


def test_index_gap_rejected():
    d = fresh()
    s = Storage(d)
    s.load()
    try:
        s.append_entries([entry(2)])
    except ValueError:
        return
    raise AssertionError("ожидали ValueError на пропуск индекса")


# ------------------------------------------------------------------ Lab 6: snapshot / сжатие
def snap_args(n, term=1):
    return n, term, {f"k{i}": i for i in range(1, n + 1)}, {"c": {"req_id": n, "index": n, "term": term, "key": f"k{n}"}}


def test_snapshot_roundtrip():
    d = fresh()
    s = Storage(d)
    s.load()
    s.save_snapshot(*snap_args(5, term=2))
    s.close()
    st = Storage(d).load()
    sn = st["snapshot"]
    assert sn["last_included_index"] == 5 and sn["last_included_term"] == 2
    assert sn["state"]["k3"] == 3 and sn["last_req"]["c"]["req_id"] == 5


def test_no_snapshot_is_none():
    assert Storage(fresh()).load()["snapshot"] is None


def test_compaction_then_reload_and_append():
    d = fresh()
    s = Storage(d)
    s.load()
    s.append_entries([entry(i) for i in range(1, 9)])
    s.save_snapshot(*snap_args(5))
    s.compact_log([entry(6), entry(7), entry(8)], 5)
    s.append_entries([entry(9)])                         # лог продолжается с учётом base
    s.close()
    st = Storage(d).load()
    assert st["snapshot"]["last_included_index"] == 5
    assert [e["index"] for e in st["log"]] == [6, 7, 8, 9], st["log"]


def test_truncate_after_compaction():
    d = fresh()
    s = Storage(d)
    s.load()
    s.append_entries([entry(i) for i in range(1, 7)])
    s.save_snapshot(*snap_args(3))
    s.compact_log([entry(4), entry(5), entry(6)], 3)
    s.truncate_from(5)                                   # режем 5, 6
    s.append_entries([entry(5, term=2)])
    s.close()
    log = Storage(d).load()["log"]
    assert [(e["index"], e["term"]) for e in log] == [(4, 1), (5, 2)], log


def test_crash_between_snapshot_and_compaction():
    """snapshot(5) записан, а log.jsonl ещё полный (1..8): load() достраивает обрезку."""
    d = fresh()
    s = Storage(d)
    s.load()
    s.append_entries([entry(i) for i in range(1, 9)])
    s.save_snapshot(*snap_args(5))                       # <- крах здесь, compact_log не вызван
    s.close()
    s2 = Storage(d)
    st = s2.load()
    assert [e["index"] for e in st["log"]] == [6, 7, 8], st["log"]
    assert st["snapshot"]["last_included_index"] == 5
    s2.append_entries([entry(9)])
    s2.close()
    assert [e["index"] for e in Storage(d).load()["log"]] == [6, 7, 8, 9]
    # и после перезагрузки файл реально укорочен
    assert sum(1 for _ in open(os.path.join(d, "log.jsonl"), "rb")) == 4


def test_snapshot_covers_whole_log():
    d = fresh()
    s = Storage(d)
    s.load()
    s.append_entries([entry(i) for i in range(1, 4)])
    s.save_snapshot(*snap_args(10))                      # snapshot ушёл вперёд лога (InstallSnapshot)
    s.close()
    s2 = Storage(d)
    st = s2.load()
    assert st["log"] == [] and st["commit_index"] == 10
    s2.append_entries([entry(11)])                       # следующий индекс = 11
    s2.close()
    assert [e["index"] for e in Storage(d).load()["log"]] == [11]


def test_compacted_log_without_snapshot_is_error():
    d = fresh()
    s = Storage(d)
    s.load()
    s.append_entries([entry(i) for i in range(1, 6)])
    s.save_snapshot(*snap_args(3))
    s.compact_log([entry(4), entry(5)], 3)
    s.close()
    os.remove(os.path.join(d, "snapshot.json"))
    try:
        Storage(d).load()
    except StorageError:
        return
    raise AssertionError("ожидали StorageError: лог обрезан, а snapshot потерян")


def test_corrupt_snapshot_ignored_when_log_complete():
    d = fresh()
    s = Storage(d)
    s.load()
    s.append_entries([entry(1), entry(2)])
    s.close()
    open(os.path.join(d, "snapshot.json"), "w").write("{broken")
    s2 = Storage(d)
    st = s2.load()
    assert st["snapshot"] is None and s2.stats["snapshot_corrupt"] and len(st["log"]) == 2


def test_snapshot_tmp_leftover_ignored():
    """крах посреди записи snapshot'а: остался snapshot.json.tmp - старый snapshot цел."""
    d = fresh()
    s = Storage(d)
    s.load()
    s.save_snapshot(*snap_args(4))
    s.close()
    open(os.path.join(d, "snapshot.json.tmp"), "w").write('{"last_included_index": 99, "sta')
    assert Storage(d).load()["snapshot"]["last_included_index"] == 4


def test_commit_not_below_snapshot():
    d = fresh()
    s = Storage(d)
    s.load()
    s.save_snapshot(*snap_args(7))
    s.close()
    assert Storage(d).load()["commit_index"] == 7


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
        print(f"OK  {t.__name__}")
    print(f"\nВсе {len(tests)} тестов Storage пройдены")
