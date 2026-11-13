#!/usr/bin/env python3
"""
Проверки snapshot'ов на ОДНОМ узле, без сети (шаг 4 задания):  python test_snapshot_local.py

Кластер из одного узла (majority = 1) сам выбирает себя лидером и сам коммитит, поэтому через
handle_put можно прогнать много записей и посмотреть: snapshot -> обрезка -> рестарт -> state/log_len.
Затем вручную вызываются on_append_entries / on_request_vote - проверка prevLogIndex и
last_log_index/term после обрезки лога.
"""
import logging
import shutil
import tempfile

import node as nd
from node import Node

logging.disable(logging.CRITICAL)
CLUSTER1 = {1: ("127.0.0.1", 9999)}


def new_node(d, threshold=10, cluster=CLUSTER1):
    return Node(1, cluster, d, snapshot_threshold=threshold, metrics_interval=3600)


def lead(n):
    n.start_election()
    assert n.role == "leader"


def put(n, k, v, cid="c", rid=1):
    r = n.handle_put({"type": "PUT", "key": k, "value": v, "client_id": cid, "req_id": rid})
    assert r["type"] == "PUT_OK", r
    return r


def test_snapshot_created_and_log_shrinks():
    d = tempfile.mkdtemp()
    n = new_node(d, threshold=10)
    lead(n)
    for i in range(1, 36):
        put(n, f"k{i}", i, rid=i)
    assert n.snapshots_taken >= 3, n.snapshots_taken
    assert n.snap_idx > 0 and len(n.log) < 10, (n.snap_idx, len(n.log))
    assert n.last_log_index() == 36                       # NOOP + 35 PUT
    assert n.state == {f"k{i}": i for i in range(1, 36)}
    assert n.log[0]["index"] == n.snap_idx + 1
    print(f"    snap_idx={n.snap_idx} log_len={len(n.log)} last_idx={n.last_log_index()} "
          f"snapshots={n.snapshots_taken}")


def test_restart_restores_state_from_snapshot_plus_tail():
    d = tempfile.mkdtemp()
    n = new_node(d, threshold=10)
    lead(n)
    for i in range(1, 36):
        put(n, f"k{i}", i, rid=i)
    snap_idx, last, log_len = n.snap_idx, n.last_log_index(), len(n.log)
    n.storage.close()
    del n                                                  # "kill": объект брошен, всё что нужно - на диске
    m = new_node(d, threshold=10)
    assert m.snap_idx == snap_idx and m.last_log_index() == last and len(m.log) == log_len
    assert m.state == {f"k{i}": i for i in range(1, 36)}, "state не совпал после рестарта"
    assert m.last_applied == m.commit_index == last
    assert m.term_at(snap_idx) == m.snap_term and m.term_at(snap_idx - 1) is None
    assert log_len < 36, "лог после рестарта должен быть короче истории"
    print(f"    после рестарта: snap_idx={m.snap_idx} log_len={len(m.log)} (история {last}) "
          f"recovery={m.recovery_ms} мс")


def test_idempotency_survives_snapshot_and_restart():
    d = tempfile.mkdtemp()
    n = new_node(d, threshold=8)
    lead(n)
    first = put(n, "x", 1, cid="alice", rid=1)
    for i in range(2, 30):                                 # чужая нагрузка -> snapshot уйдёт за запись alice
        put(n, f"k{i}", i, cid="bob", rid=i)
    assert n.snap_idx > first["index"], "запись alice должна оказаться внутри snapshot'а"
    n.storage.close()
    m = new_node(d, threshold=8)
    lead(m)
    again = m.handle_put({"type": "PUT", "key": "x", "value": 999, "client_id": "alice", "req_id": 1})
    assert again["type"] == "PUT_OK" and again["duplicate"] is True
    assert again["index"] == first["index"], (again, first)
    assert m.state["x"] == 1, "значение не должно измениться"


def test_threshold_disabled_keeps_full_log():
    d = tempfile.mkdtemp()
    n = new_node(d, threshold=10 ** 9)
    lead(n)
    for i in range(1, 21):
        put(n, f"k{i}", i, rid=i)
    assert n.snap_idx == 0 and len(n.log) == 21 and n.snapshots_taken == 0


def test_vote_uses_snapshot_when_log_empty():
    """Лог пуст, но snapshot есть: last_log_index/term берутся из snapshot'а (RequestVote)."""
    d = tempfile.mkdtemp()
    cluster = {1: ("127.0.0.1", 9999), 2: ("127.0.0.1", 9998), 3: ("127.0.0.1", 9997)}
    f = Node(2, cluster, d, snapshot_threshold=1000, metrics_interval=3600)
    f.on_install_snapshot({"term": 3, "leader_id": 1, "last_included_index": 30, "last_included_term": 3,
                           "state": {"a": 1}, "last_req": {}})
    assert f.log == [] and f.last_log_index() == 30 and f.last_log_term() == 3
    # кандидат с более коротким логом (в том же last term) - отказ
    r = f.on_request_vote({"term": 4, "candidate_id": 3, "last_log_term": 3, "last_log_index": 29})
    assert r["granted"] is False
    # кандидат с более старым last term, но длинным логом - отказ
    r = f.on_request_vote({"term": 4, "candidate_id": 3, "last_log_term": 2, "last_log_index": 99})
    assert r["granted"] is False
    # кандидат не хуже - голос
    r = f.on_request_vote({"term": 4, "candidate_id": 3, "last_log_term": 3, "last_log_index": 30})
    assert r["granted"] is True


def follower_after_snapshot():
    """Follower, у которого лог 1..40 уже сжат snapshot'ом (snap_idx > 0)."""
    d = tempfile.mkdtemp()
    cluster = {1: ("127.0.0.1", 9999), 2: ("127.0.0.1", 9998), 3: ("127.0.0.1", 9997)}
    f = Node(2, cluster, d, snapshot_threshold=10, metrics_interval=3600)
    entries = [{"term": 1, "index": i, "command": {"op": "PUT", "key": f"k{i}", "value": i,
                                                   "client_id": "c", "req_id": i}} for i in range(1, 41)]
    r = f.on_append_entries({"term": 1, "leader_id": 1, "prev_log_index": 0, "prev_log_term": 0,
                             "entries": entries, "leader_commit": 40})
    assert r["success"] and r["match_index"] == 40, r
    assert f.snap_idx > 0 and f.snapshots_taken >= 1
    return f, entries


def test_append_entries_prev_equals_snapshot_boundary():
    f, entries = follower_after_snapshot()
    s = f.snap_idx
    nxt = {"term": 1, "index": 41, "command": {"op": "PUT", "key": "k41", "value": 41,
                                              "client_id": "c", "req_id": 41}}
    # prev = последняя запись (она может быть внутри хвоста или на границе snapshot'а)
    r = f.on_append_entries({"term": 1, "leader_id": 1, "prev_log_index": 40, "prev_log_term": 1,
                             "entries": [nxt], "leader_commit": 41})
    assert r["success"] and r["match_index"] == 41 and f.state["k41"] == 41
    # prev ровно на границе snapshot'а: сравнивается snap_term
    r = f.on_append_entries({"term": 1, "leader_id": 1, "prev_log_index": f.snap_idx, "prev_log_term": f.snap_term,
                             "entries": [], "leader_commit": 41})
    assert r["success"], r
    # неверный term на границе -> конфликт
    r = f.on_append_entries({"term": 1, "leader_id": 1, "prev_log_index": f.snap_idx, "prev_log_term": 99,
                             "entries": [], "leader_commit": 41})
    assert r["success"] is False
    print(f"    граница snap_idx={s}: prev==snapshot принят, неверный prev_term отвергнут")


def test_append_entries_prev_inside_snapshot_is_accepted():
    """prev_log_index < last_included_index: префикс закоммичен -> принимаем, дубликаты пропускаем."""
    f, entries = follower_after_snapshot()
    s = f.snap_idx
    assert s > 5
    before = (f.last_log_index(), len(f.log), dict(f.state))
    # лидер "переотправляет" записи 3..45 (кусок внутри snapshot'а + хвост + новые)
    resend = entries[2:] + [{"term": 1, "index": 41, "command": {"op": "PUT", "key": "k41", "value": 41,
                                                                "client_id": "c", "req_id": 41}}]
    r = f.on_append_entries({"term": 1, "leader_id": 1, "prev_log_index": 2, "prev_log_term": 1,
                             "entries": resend, "leader_commit": 41})
    assert r["success"] and r["match_index"] == 41, r
    assert f.last_log_index() == 41 and f.state["k41"] == 41
    assert f.state["k3"] == 3 and len(f.log) <= before[1] + 1, "записи внутри snapshot'а не должны дублироваться"
    print(f"    prev=2 < snap_idx={s}: принят, лог не раздулся")


def test_conflict_hint_never_below_snapshot():
    f, entries = follower_after_snapshot()
    f.on_append_entries({"term": 1, "leader_id": 1, "prev_log_index": 40, "prev_log_term": 1,
                         "entries": [{"term": 1, "index": 41, "command": {"op": "NOOP"}}], "leader_commit": 41})
    r = f.on_append_entries({"term": 2, "leader_id": 3, "prev_log_index": 41, "prev_log_term": 2,
                             "entries": [], "leader_commit": 41})
    assert r["success"] is False and r["last_log_index"] >= f.snap_idx, (r, f.snap_idx)


def test_install_snapshot_replaces_state():
    d = tempfile.mkdtemp()
    cluster = {1: ("127.0.0.1", 9999), 2: ("127.0.0.1", 9998), 3: ("127.0.0.1", 9997)}
    f = Node(2, cluster, d, snapshot_threshold=1000, metrics_interval=3600)
    f.on_append_entries({"term": 1, "leader_id": 1, "prev_log_index": 0, "prev_log_term": 0,
                         "entries": [{"term": 1, "index": 1, "command": {"op": "NOOP"}}],
                         "leader_commit": 1})
    msg = {"term": 1, "leader_id": 1, "last_included_index": 50, "last_included_term": 1,
           "state": {"a": 1, "b": 2}, "last_req": {"c": {"req_id": 7, "index": 50, "term": 1, "key": "b"}}}
    r = f.on_install_snapshot(msg)
    assert r["success"] and r["match_index"] == 50
    assert f.snap_idx == 50 and f.last_applied == f.commit_index == 50 and f.log == []
    assert f.state == {"a": 1, "b": 2} and f.last_req["c"]["req_id"] == 7
    assert f.last_log_index() == 50 and f.last_log_term() == 1
    f.storage.close()
    g = Node(2, cluster, d, snapshot_threshold=1000, metrics_interval=3600)     # рестарт
    assert g.snap_idx == 50 and g.state == {"a": 1, "b": 2} and g.last_applied == 50
    # повторная установка более старого snapshot'а - игнорируется (уже закоммичено)
    msg2 = dict(msg, last_included_index=20, state={"zzz": 1})
    assert g.on_install_snapshot(msg2)["success"] and "zzz" not in g.state


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
        print(f"OK  {t.__name__}")
    print(f"\nВсе {len(tests)} локальных тестов snapshot'ов пройдены")
