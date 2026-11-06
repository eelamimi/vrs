#!/usr/bin/env python3
"""
Lab 5. Персистентность лога, восстановление после сбоя, идемпотентные клиентские операции.
Развитие Lab 4 (упрощённый Raft).

Запуск:
    python node.py 1
    python node.py 2
    python node.py 3
    python node.py 2 --cluster 1:9001,2:9002,3:9003,4:9004,5:9005
    python node.py 1 --data-dir data          # каталог узла: data/node1/  (по умолчанию)
    python node.py 1 --data data/node1        # либо явный путь каталога узла

Что добавлено относительно Lab 4:
  * Storage (storage.py): meta.json (currentTerm, votedFor), log.jsonl (append-only, CRC32,
    fsync на каждую запись), commit.json (подсказка commit_index).
  * Правило "сначала на диск, потом протокол": term/votedFor/log сохраняются синхронно ДО
    ответа на RequestVote/AppendEntries и до продолжения работы лидера.
  * Восстановление при старте: load -> commit_index (из подсказки, не больше длины лога) ->
    state machine и last_req пересобираются повторным применением записей лога 1..commit_index.
    Хвост лога после commit_index НЕ применяется (он может оказаться неподтверждённым):
    он применится, когда лидер сообщит commit.
  * Идемпотентность: PUT несёт client_id + req_id. Таблица last_req[client_id] =
    {req_id, index, term, key} меняется ТОЛЬКО в apply, поэтому после рестарта восстанавливается
    вместе со state machine. Повтор req_id возвращает исходный результат (тот же index).
    Запросы, уже лежащие в логе, но ещё не закоммиченные, дедуплицируются по (client_id, req_id)
    через индекс по логу (req_map) - они не добавляются повторно, а просто ждут commit.
    Дубликат, всё же попавший в лог, пропускается в apply (эффект не удваивается).

Сообщения (кроме Lab 4):
    PUT {key, value, client_id, req_id} -> PUT_OK {index, term, duplicate} | ERROR
    DUMP {full?}                        -> состояние узла + метрики хранилища
    CRASH_POINT {point}                 -> fault injection: after_commit | before_replicate | none
"""
import argparse
import json
import logging
import os
import random
import socket
import threading
import time

from storage import Storage

HEARTBEAT_INTERVAL = 0.5
ELECTION_TIMEOUT = (2.5, 4.5)
RPC_TIMEOUT = 1.0
PUT_TIMEOUT = 5.0
MAX_BATCH = 50

DEFAULT_CLUSTER = "1:9001,2:9002,3:9003"


def parse_cluster(s):
    cluster = {}
    for item in s.split(","):
        nid, port = item.strip().split(":")
        cluster[int(nid)] = ("127.0.0.1", int(port))
    return cluster


class Node:
    def __init__(self, node_id, cluster, node_dir):
        self.node_id = node_id
        self.cluster = cluster
        self.peers = {i: a for i, a in cluster.items() if i != node_id}
        self.port = cluster[node_id][1]
        self.majority = len(cluster) // 2 + 1

        # --- volatile
        self.state = {}                # KV state machine
        self.last_req = {}             # client_id -> {req_id, index, term, key}  (только через apply)
        self.req_map = {}              # (client_id, req_id) -> index записи в логе
        self.last_applied = 0
        self.role = "follower"
        self.leader_id = None
        self.next_index = {}
        self.match_index = {}
        self.noop_index = 0
        self.wake = {}

        self.lock = threading.RLock()
        self.cond = threading.Condition(self.lock)
        self.last_heartbeat = time.time()
        self.election_timeout = random.uniform(*ELECTION_TIMEOUT)
        self.crash_point = None

        # --- persistent: загрузка с диска
        self.node_dir = node_dir
        self.storage = Storage(node_dir)
        saved = self.storage.load()
        self.term = saved["current_term"]
        self.voted_for = saved["voted_for"]
        self.log = saved["log"]
        self.commit_index = saved["commit_index"]
        self._rebuild_req_map()
        self._recover()

    # ------------------------------------------------------------ логирование
    def log_msg(self, text):
        logging.info("[node %d | term %d | %-9s | last_idx=%d commit=%d applied=%d] %s",
                     self.node_id, self.term, self.role, self.last_log_index(),
                     self.commit_index, self.last_applied, text)

    def _recover(self):
        """Пересобрать state machine и last_req из лога (записи 1..commit_index)."""
        t0 = time.time()
        self.apply_committed(quiet=True)
        st = self.storage.stats
        fresh = (self.term == 0 and not self.log)
        logging.info("[node %d] СТАРТ %s: data_dir=%s term=%d voted_for=%s log_len=%d "
                     "commit_index=%d applied=%d ключей=%d recovery=%.1f мс "
                     "(чтение диска %.1f мс, отброшено байт хвоста: %d)",
                     self.node_id, "С НУЛЯ" if fresh else "С ДИСКА", self.node_dir, self.term,
                     self.voted_for, len(self.log), self.commit_index, self.last_applied,
                     len(self.state), (time.time() - t0) * 1000 + st["recovery_ms"],
                     st["recovery_ms"], st["dropped_bytes"])
        if st["meta_corrupt"]:
            logging.warning("[node %d] meta.json повреждён - term/votedFor сброшены", self.node_id)

    # ------------------------------------------------------------ сохранение (синхронно)
    def _save_meta(self):
        """Под self.lock: term/votedFor на диск (fsync) до продолжения протокола."""
        self.storage.save_hard_state(self.term, self.voted_for)

    def _save_commit(self):
        self.storage.save_commit(self.commit_index)

    # ------------------------------------------------------------ сеть
    def send(self, peer_id, msg, timeout=RPC_TIMEOUT):
        host, port = self.cluster[peer_id]
        try:
            with socket.create_connection((host, port), timeout=timeout) as s:
                s.settimeout(timeout)
                s.sendall((json.dumps(msg) + "\n").encode("utf-8"))
                line = s.makefile("r", encoding="utf-8").readline()
            return json.loads(line) if line else None
        except (OSError, ValueError):
            return None

    def broadcast(self, msg):
        results = {}

        def worker(pid):
            results[pid] = self.send(pid, msg)

        threads = [threading.Thread(target=worker, args=(pid,), daemon=True) for pid in self.peers]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return results

    # ------------------------------------------------------------ лог и state machine
    def last_log_index(self):
        return len(self.log)

    def last_log_term(self):
        return self.log[-1]["term"] if self.log else 0

    def _rebuild_req_map(self):
        self.req_map = {}
        for e in self.log:
            c = e["command"]
            if c.get("client_id") is not None:
                self.req_map[(c["client_id"], c["req_id"])] = e["index"]

    def append_command(self, command):
        """Только лидер, под self.lock. Запись попадает на диск (fsync) до возврата."""
        entry = {"term": self.term, "index": len(self.log) + 1, "command": command}
        self.log.append(entry)
        self.storage.append_entries([entry])
        if command.get("client_id") is not None:
            self.req_map[(command["client_id"], command["req_id"])] = entry["index"]
        return entry

    def apply_committed(self, quiet=False):
        """Под self.lock. last_req обновляется ТОЛЬКО здесь."""
        while self.last_applied < self.commit_index:
            entry = self.log[self.last_applied]
            cmd = entry["command"]
            idx = entry["index"]
            if cmd["op"] == "PUT":
                cid, rid = cmd["client_id"], cmd["req_id"]
                sess = self.last_req.get(cid)
                if sess and rid <= sess["req_id"]:
                    if not quiet:
                        self.log_msg(f"APPLY idx={idx} PUT {cmd['key']}: дубликат "
                                     f"({cid}, {rid}) - эффект пропущен")
                else:
                    self.state[cmd["key"]] = cmd["value"]
                    self.last_req[cid] = {"req_id": rid, "index": idx,
                                          "term": entry["term"], "key": cmd["key"]}
                    if not quiet:
                        self.log_msg(f"APPLY idx={idx} PUT {cmd['key']} -> "
                                     f"state[{cmd['key']}]={cmd['value']!r} (client {cid}, req {rid})")
            elif not quiet:
                self.log_msg(f"APPLY idx={idx} {cmd['op']}")
            self.last_applied += 1

    # ------------------------------------------------------------ term / роли
    def _step_down(self, term, leader=None):
        with self.lock:
            if term > self.term:
                self.term = term
                self.voted_for = None
                self._save_meta()
            was = self.role
            self.role = "follower"
            if leader is not None:
                self.leader_id = leader
            self.last_heartbeat = time.time()
            self.cond.notify_all()
        if was != "follower":
            self.log_msg(f"перешёл в follower (term {term})")

    # ------------------------------------------------------------ выборы
    def start_election(self):
        with self.lock:
            if self.role == "leader":
                return
            self.term += 1
            self.role = "candidate"
            self.voted_for = self.node_id
            self.leader_id = None
            self._save_meta()                      # term++ и голос за себя - на диск до рассылки
            my_term = self.term
            msg = {"type": "REQUEST_VOTE", "term": my_term, "candidate_id": self.node_id,
                   "last_log_index": self.last_log_index(), "last_log_term": self.last_log_term()}
        self.log_msg("начинаю выборы")
        replies = self.broadcast(msg)
        votes = 1
        for r in replies.values():
            if not r:
                continue
            if r.get("term", 0) > my_term:
                self._step_down(r["term"])
                return
            if r.get("granted"):
                votes += 1
        with self.lock:
            if self.role == "candidate" and self.term == my_term and votes >= self.majority:
                self._become_leader()
            else:
                self.log_msg(f"выборы не выиграны: голосов {votes}/{len(self.cluster)}")
                if self.role == "candidate":
                    self.role = "follower"

    def _become_leader(self):
        """Под self.lock."""
        self.role = "leader"
        self.leader_id = self.node_id
        self.next_index = {p: len(self.log) + 1 for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        noop = self.append_command({"op": "NOOP"})
        self.noop_index = noop["index"]
        self.log_msg("*** Я ЛИДЕР *** (записал NOOP idx=%d)" % self.noop_index)
        term = self.term
        self.wake = {}
        for p in self.peers:
            ev = threading.Event()
            self.wake[p] = ev
            threading.Thread(target=self._replicator, args=(p, term, ev), daemon=True).start()
        self._advance_commit()

    def on_request_vote(self, msg):
        with self.lock:
            if msg["term"] < self.term:
                return {"type": "REQUEST_VOTE_REPLY", "term": self.term, "granted": False}
            changed = False
            if msg["term"] > self.term:
                self.term = msg["term"]
                self.voted_for = None
                changed = True
                if self.role != "follower":
                    self.role = "follower"
                    self.cond.notify_all()
            up_to_date = (msg["last_log_term"], msg["last_log_index"]) >= \
                         (self.last_log_term(), self.last_log_index())
            granted = up_to_date and self.voted_for in (None, msg["candidate_id"])
            if granted:
                if self.voted_for != msg["candidate_id"]:
                    changed = True
                self.voted_for = msg["candidate_id"]
                self.last_heartbeat = time.time()
            if changed:
                self._save_meta()                  # ДО ответа: иначе после рестарта можно проголосовать дважды
            self.log_msg(f"REQUEST_VOTE от {msg['candidate_id']} (term {msg['term']}, "
                         f"его лог {msg['last_log_term']}/{msg['last_log_index']}): "
                         f"{'ГОЛОС ОТДАН' if granted else 'отказ'}")
            return {"type": "REQUEST_VOTE_REPLY", "term": self.term, "granted": granted}

    # ------------------------------------------------------------ репликация (лидер)
    def _replicator(self, peer, term, ev):
        while True:
            with self.lock:
                if self.role != "leader" or self.term != term:
                    return
            progressed = self._replicate_to(peer, term)
            if progressed:
                with self.lock:
                    more = self.next_index.get(peer, 1) <= len(self.log)
                if more:
                    continue
            ev.wait(HEARTBEAT_INTERVAL)
            ev.clear()

    def _replicate_to(self, peer, term):
        with self.lock:
            if self.role != "leader" or self.term != term:
                return False
            ni = self.next_index[peer]
            prev_idx = ni - 1
            prev_term = self.log[prev_idx - 1]["term"] if prev_idx > 0 else 0
            entries = self.log[ni - 1: ni - 1 + MAX_BATCH]
            msg = {"type": "APPEND_ENTRIES", "term": term, "leader_id": self.node_id,
                   "prev_log_index": prev_idx, "prev_log_term": prev_term,
                   "entries": entries, "leader_commit": self.commit_index}
        r = self.send(peer, msg)
        if not r:
            return False
        with self.lock:
            if r.get("term", 0) > self.term:
                self.log_msg(f"узел {peer} в term {r['term']} - я устарел")
                self._step_down(r["term"])
                return False
            if self.role != "leader" or self.term != term:
                return False
            if r.get("success"):
                self.match_index[peer] = max(self.match_index[peer], r["match_index"])
                self.next_index[peer] = self.match_index[peer] + 1
                self._advance_commit()
            else:
                hint = r.get("last_log_index", ni - 2)
                self.next_index[peer] = max(1, min(ni - 1, hint + 1))
                self.log_msg(f"узел {peer}: лог не совпал на prev={prev_idx}, "
                             f"next_index -> {self.next_index[peer]}")
        return True

    def _advance_commit(self):
        """Под self.lock. N коммитится, если он на большинстве И из текущего term."""
        for n in range(len(self.log), self.commit_index, -1):
            if self.log[n - 1]["term"] != self.term:
                break
            count = 1 + sum(1 for m in self.match_index.values() if m >= n)
            if count >= self.majority:
                self.commit_index = n
                self._save_commit()
                self.log_msg(f"COMMIT idx={n} ({count}/{len(self.cluster)})")
                break
        self.apply_committed()
        self.cond.notify_all()

    # ------------------------------------------------------------ репликация (follower)
    def on_append_entries(self, msg):
        with self.lock:
            def reply(success, match=0, hint=None):
                return {"type": "APPEND_ENTRIES_REPLY", "term": self.term, "from_id": self.node_id,
                        "success": success, "match_index": match,
                        "last_log_index": self.last_log_index() if hint is None else hint}

            if msg["term"] < self.term:
                return reply(False)
            if msg["term"] > self.term:
                self.term = msg["term"]
                self.voted_for = None
                self._save_meta()
            if self.role != "follower":
                self.log_msg(f"признаю лидера {msg['leader_id']}, становлюсь follower")
                self.cond.notify_all()
            self.role = "follower"
            self.leader_id = msg["leader_id"]
            self.last_heartbeat = time.time()

            prev_idx, prev_term = msg["prev_log_index"], msg["prev_log_term"]
            if prev_idx > len(self.log):
                return reply(False, hint=len(self.log))
            if prev_idx > 0 and self.log[prev_idx - 1]["term"] != prev_term:
                ct = self.log[prev_idx - 1]["term"]
                i = prev_idx
                while i > 1 and self.log[i - 2]["term"] == ct:
                    i -= 1
                self.log_msg(f"конфликт на prev_idx={prev_idx} (term {ct} != {prev_term})")
                return reply(False, hint=i - 1)

            new, truncated = [], False
            for k, e in enumerate(msg["entries"]):
                idx = prev_idx + 1 + k
                if idx <= len(self.log):
                    if self.log[idx - 1]["term"] == e["term"]:
                        continue
                    self.log_msg(f"обрезаю хвост лога начиная с idx={idx} (конфликт term)")
                    del self.log[idx - 1:]
                    self.storage.truncate_from(idx)          # диск: усечение + fsync
                    truncated = True
                self.log.append(e)
                new.append(e)
            if new:
                self.storage.append_entries(new)            # диск: append + fsync ДО ответа лидеру
                self.log_msg(f"принял записи idx {new[0]['index']}..{new[-1]['index']} (на диске)")
            if truncated:
                self._rebuild_req_map()
            elif new:
                for e in new:
                    c = e["command"]
                    if c.get("client_id") is not None:
                        self.req_map[(c["client_id"], c["req_id"])] = e["index"]

            match = prev_idx + len(msg["entries"])
            if msg["leader_commit"] > self.commit_index:
                self.commit_index = min(msg["leader_commit"], match)
                self._save_commit()
                self.apply_committed()
            return reply(True, match)

    # ------------------------------------------------------------ клиентские операции
    def _not_leader(self):
        return {"type": "ERROR", "error": "not_leader", "leader": self.leader_id,
                "message": f"I am not leader, current leader is {self.leader_id}"}

    def _put_ok(self, sess, duplicate):
        return {"type": "PUT_OK", "key": sess["key"], "index": sess["index"],
                "term": sess["term"], "duplicate": duplicate}

    def handle_put(self, msg):
        key, cid, rid = msg.get("key"), msg.get("client_id"), msg.get("req_id")
        if key is None or cid is None or not isinstance(rid, int):
            return {"type": "ERROR", "error": "bad_request",
                    "message": "нужны key, client_id и целочисленный req_id"}
        with self.lock:
            if self.role != "leader":
                return self._not_leader()
            term = self.term

            # --- дедупликация: уже применённый запрос
            sess = self.last_req.get(cid)
            if sess and rid == sess["req_id"]:
                self.log_msg(f"PUT ({cid}, {rid}): повтор, уже применён на idx={sess['index']} "
                             f"- возвращаю прежний результат")
                return self._put_ok(sess, True)
            if sess and rid < sess["req_id"]:
                return {"type": "ERROR", "error": "stale_request",
                        "message": f"req_id {rid} старше последнего обработанного {sess['req_id']}"}

            # --- дедупликация: запрос уже в логе, но ещё не применён
            idx = self.req_map.get((cid, rid))
            if idx is None:
                entry = self.append_command({"op": "PUT", "key": key, "value": msg.get("value"),
                                             "client_id": cid, "req_id": rid})
                idx = entry["index"]
                self.log_msg(f"PUT {key}={msg.get('value')!r} ({cid}, {rid}): "
                             f"записано в лог idx={idx} (fsync), жду commit")
            else:
                self.log_msg(f"PUT ({cid}, {rid}): уже в логе idx={idx}, "
                             f"новую запись НЕ добавляю, жду commit")
            if self.crash_point == "before_replicate":
                self.log_msg("CRASH POINT before_replicate: убиваю процесс")
                os._exit(1)
            for ev in self.wake.values():
                ev.set()

            deadline = time.time() + PUT_TIMEOUT
            while True:
                if self.role != "leader" or self.term != term:
                    return {"type": "ERROR", "error": "leader_changed", "leader": self.leader_id,
                            "message": "leadership lost; outcome unknown, retry with same req_id"}
                if self.commit_index >= idx and self.last_applied >= idx:
                    sess = self.last_req.get(cid)
                    if sess and sess["req_id"] == rid:
                        if self.crash_point == "after_commit":
                            self.log_msg("CRASH POINT after_commit: убиваю процесс до ответа клиенту")
                            os._exit(1)
                        self.log_msg(f"PUT ({cid}, {rid}) подтверждён: idx={sess['index']}")
                        return self._put_ok(sess, False)
                    return {"type": "ERROR", "error": "leader_changed",
                            "message": "entry was overwritten, retry"}
                remaining = deadline - time.time()
                if remaining <= 0:
                    return {"type": "ERROR", "error": "timeout", "leader": self.node_id,
                            "message": "no commit within timeout (no quorum?)"}
                self.cond.wait(remaining)

    def _confirm_leadership(self, term):
        msg = {"type": "APPEND_ENTRIES", "term": term, "leader_id": self.node_id,
               "prev_log_index": 0, "prev_log_term": 0, "entries": [], "leader_commit": 0}
        replies = self.broadcast(msg)
        ok = 1
        for r in replies.values():
            if not r:
                continue
            if r.get("term", 0) > term:
                self._step_down(r["term"])
                return False
            if r.get("term") == term:
                ok += 1
        return ok >= self.majority

    def on_get(self, msg):
        with self.lock:
            if self.role != "leader":
                return self._not_leader()
            term = self.term
            deadline = time.time() + PUT_TIMEOUT
            while self.commit_index < self.noop_index:
                remaining = deadline - time.time()
                if remaining <= 0 or self.role != "leader":
                    return {"type": "ERROR", "error": "not_ready", "leader": self.node_id}
                self.cond.wait(remaining)
        if not self._confirm_leadership(term):
            with self.lock:
                return self._not_leader()
        with self.lock:
            if self.role != "leader" or self.term != term:
                return self._not_leader()
            key = msg["key"]
            return {"type": "GET_OK", "key": key, "found": key in self.state,
                    "value": self.state.get(key), "commit_index": self.commit_index}

    def on_dump(self, msg):
        with self.lock:
            out = {"type": "DUMP_OK", "node_id": self.node_id, "state": self.role,
                   "term": self.term, "voted_for": self.voted_for, "leader": self.leader_id,
                   "log_len": len(self.log), "commit_index": self.commit_index,
                   "last_applied": self.last_applied, "data": dict(self.state),
                   "last_req": {c: s["req_id"] for c, s in self.last_req.items()},
                   "storage": dict(self.storage.stats), "data_dir": self.node_dir}
            if msg.get("full"):
                out["log"] = [{"index": e["index"], "term": e["term"], "op": e["command"]["op"],
                               "key": e["command"].get("key"),
                               "client_id": e["command"].get("client_id"),
                               "req_id": e["command"].get("req_id")} for e in self.log]
            return out

    def on_crash_point(self, msg):
        self.crash_point = msg.get("point") or None
        self.log_msg(f"crash_point = {self.crash_point}")
        return {"type": "OK", "crash_point": self.crash_point}

    # ------------------------------------------------------------ фоновые циклы и запуск
    def election_timeout_loop(self):
        while True:
            time.sleep(0.1)
            with self.lock:
                if self.role == "leader":
                    continue
                expired = time.time() - self.last_heartbeat > self.election_timeout
                if expired:
                    self.election_timeout = random.uniform(*ELECTION_TIMEOUT)
                    self.last_heartbeat = time.time()
            if expired:
                self.log_msg("heartbeat потерян - выборы")
                self.start_election()

    def handle_message(self, msg):
        handlers = {
            "REQUEST_VOTE": self.on_request_vote,
            "APPEND_ENTRIES": self.on_append_entries,
            "PUT": self.handle_put,
            "GET": self.on_get,
            "DUMP": self.on_dump,
            "CRASH_POINT": self.on_crash_point,
        }
        handler = handlers.get(msg.get("type"))
        if handler is None:
            return {"type": "ERROR", "error": "unknown_type"}
        return handler(msg)

    def _handle_conn(self, conn, addr):
        with conn:
            conn.settimeout(PUT_TIMEOUT + 5)
            try:
                line = conn.makefile("r", encoding="utf-8").readline()
                if not line:
                    return
                reply = self.handle_message(json.loads(line))
            except Exception as e:  # noqa: BLE001
                reply = {"type": "ERROR", "error": f"bad_request: {e}"}
            try:
                conn.sendall((json.dumps(reply) + "\n").encode("utf-8"))
            except OSError:
                pass

    def serve(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", self.port))
        srv.listen(64)
        self.log_msg(f"слушаю порт {self.port}, peers: {sorted(self.peers)}")
        while True:
            conn, addr = srv.accept()
            threading.Thread(target=self._handle_conn, args=(conn, addr), daemon=True).start()

    def start(self):
        threading.Thread(target=self.serve, daemon=True).start()
        threading.Thread(target=self.election_timeout_loop, daemon=True).start()
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            self.log_msg("остановлен (Ctrl+C)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Lab 5: узел кластера (Raft + персистентность)")
    ap.add_argument("node_id", type=int)
    ap.add_argument("--cluster", default=DEFAULT_CLUSTER, help="id:port,id:port,...")
    ap.add_argument("--data-dir", default="data", help="корень; каталог узла = <root>/node<id>")
    ap.add_argument("--data", default=None, help="явный каталог узла (приоритетнее --data-dir)")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    cluster = parse_cluster(args.cluster)
    if args.node_id not in cluster:
        raise SystemExit(f"ID {args.node_id} нет в --cluster")
    node_dir = args.data or os.path.join(args.data_dir, f"node{args.node_id}")
    Node(args.node_id, cluster, node_dir).start()