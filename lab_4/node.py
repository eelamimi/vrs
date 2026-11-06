#!/usr/bin/env python3
"""
Lab 4. Упрощённый Raft: репликация лога, commit index, применение к KV state machine.

Запуск (в отдельных терминалах):
    python node.py 1
    python node.py 2
    python node.py 3
Кластер по умолчанию: 1:9001,2:9002,3:9003. Для 5 узлов:
    python node.py 2 --cluster 1:9001,2:9002,3:9003,4:9004,5:9005

Протокол: одна строка JSON на запрос, одна строка JSON в ответ (TCP).

Сообщения между узлами:
    REQUEST_VOTE   {term, candidate_id, last_log_index, last_log_term}
                   -> REQUEST_VOTE_REPLY {term, granted}
    APPEND_ENTRIES {term, leader_id, prev_log_index, prev_log_term, entries, leader_commit}
                   -> APPEND_ENTRIES_REPLY {term, from_id, success, match_index, last_log_index}
Сообщения клиента:
    PUT {key, value, op_id} -> PUT_OK {index, term} | ERROR(not_leader / leader_changed / timeout)
    GET {key}               -> GET_OK | ERROR(not_leader / not_ready)
    DUMP {}                 -> DUMP_OK (состояние узла для отладки)
    CRASH_POINT {point}     -> OK  (fault injection для экспериментов: узел сам убьёт себя)

Что изменилось относительно Lab 3 (и почему):
  * Лог записей {term, index, command} + commit_index + last_applied + state (KV).
  * Репликация через APPEND_ENTRIES (prev_log_index/term, конфликт -> обрезка хвоста).
  * Лидер двигает commit_index, когда запись есть на большинстве И принадлежит текущему term.
  * PUT_OK только после commit и apply на лидере.
  * Выборы: Bully из Lab 3 заменён на Raft-голосование (REQUEST_VOTE). Причина: Bully выбирает
    узел с наибольшим ID, а он может иметь устаревший лог и потерять закоммиченные записи.
    Голос отдаётся только кандидату с логом "не хуже" своего, один голос на term.
  * Новый лидер сразу пишет NOOP-запись своего term (иначе записи прошлых term не коммитятся).
  * GET: только лидер, только после commit NOOP и после подтверждения лидерства большинством.
  * Идемпотентность retry: op_id клиента; дубликат не добавляется в лог и не применяется дважды.
"""
import argparse
import json
import logging
import os
import random
import socket
import threading
import time
import uuid

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
    def __init__(self, node_id, cluster, data_dir="data"):
        self.node_id = node_id
        self.cluster = cluster
        self.peers = {i: a for i, a in cluster.items() if i != node_id}
        self.port = cluster[node_id][1]
        self.majority = len(cluster) // 2 + 1

        # --- персистентное состояние (Raft): term, voted_for, log
        self.term = 0
        self.voted_for = None
        self.log = []                  # [{term, index, command}], index с 1 -> log[index-1]

        # --- волатильное состояние
        self.commit_index = 0
        self.last_applied = 0
        self.state = {}                # KV state machine
        self.applied_ops = set()       # op_id уже применённых PUT (дедупликация)
        self.op_map = {}               # op_id -> index записи в логе (идемпотентность retry)

        self.role = "follower"         # follower | candidate | leader
        self.leader_id = None
        self.next_index = {}           # только лидер
        self.match_index = {}          # только лидер
        self.noop_index = 0            # индекс NOOP текущего лидерства

        self.lock = threading.RLock()
        self.cond = threading.Condition(self.lock)   # будит PUT-ожидатели при commit/step_down
        self.last_heartbeat = time.time()
        self.election_timeout = random.uniform(*ELECTION_TIMEOUT)
        self.crash_point = None

        os.makedirs(data_dir, exist_ok=True)
        self.path = os.path.join(data_dir, f"node{node_id}.json")
        self._load()

    # ------------------------------------------------------------ логирование
    def log_msg(self, text):
        logging.info("[node %d | term %d | %-9s | last_idx=%d commit=%d applied=%d] %s",
                     self.node_id, self.term, self.role, self.last_log_index(),
                     self.commit_index, self.last_applied, text)

    # ------------------------------------------------------------ persistence
    def _load(self):
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path, encoding="utf-8") as f:
                saved = json.load(f)
            self.term = saved.get("term", 0)
            self.voted_for = saved.get("voted_for")
            self.log = saved.get("log", [])
            self._rebuild_op_map()
            logging.info("[node %d] загружено с диска: term=%d, записей в логе=%d "
                         "(commit_index=0: восстановится от лидера)",
                         self.node_id, self.term, len(self.log))
        except (OSError, ValueError) as e:
            logging.warning("[node %d] не удалось прочитать %s: %s", self.node_id, self.path, e)

    def _persist(self):
        """Под self.lock. Атомарная запись: tmp + rename."""
        tmp = self.path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"term": self.term, "voted_for": self.voted_for, "log": self.log}, f)
            os.replace(tmp, self.path)
        except OSError as e:
            logging.warning("[node %d] ошибка записи на диск: %s", self.node_id, e)

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

    def _rebuild_op_map(self):
        self.op_map = {}
        for e in self.log:
            oid = e["command"].get("op_id")
            if oid:
                self.op_map[oid] = e["index"]

    def append_command(self, command):
        """Только лидер, под self.lock. Возвращает новую запись."""
        entry = {"term": self.term, "index": len(self.log) + 1, "command": command}
        self.log.append(entry)
        if command.get("op_id"):
            self.op_map[command["op_id"]] = entry["index"]
        self._persist()
        return entry

    def apply_committed(self):
        """Под self.lock. while last_applied < commit_index: apply."""
        while self.last_applied < self.commit_index:
            entry = self.log[self.last_applied]
            cmd = entry["command"]
            if cmd["op"] == "PUT":
                if cmd.get("op_id") in self.applied_ops:
                    self.log_msg(f"APPLY idx={entry['index']} {cmd['key']}: дубликат op_id, пропуск")
                else:
                    self.state[cmd["key"]] = cmd["value"]
                    if cmd.get("op_id"):
                        self.applied_ops.add(cmd["op_id"])
                    self.last_applied += 1
                    self.log_msg(f"APPLY idx={entry['index']} PUT {cmd['key']} -> "
                                 f"state[{cmd['key']}]={self.state[cmd['key']]!r}")
                    continue
            else:
                self.log_msg(f"APPLY idx={entry['index']} {cmd['op']}")
            self.last_applied += 1

    # ------------------------------------------------------------ term / роли
    def _step_down(self, term, leader=None):
        with self.lock:
            if term > self.term:
                self.term = term
                self.voted_for = None
                self._persist()
            was = self.role
            self.role = "follower"
            if leader is not None:
                self.leader_id = leader
            self.last_heartbeat = time.time()
            self.cond.notify_all()
        if was != "follower":
            self.log_msg(f"перешёл в follower (term {term})")

    # ------------------------------------------------------------ выборы (Raft-голосование)
    def start_election(self):
        with self.lock:
            if self.role == "leader":
                return
            self.term += 1
            self.role = "candidate"
            self.voted_for = self.node_id
            self.leader_id = None
            self._persist()
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
        noop = self.append_command({"op": "NOOP", "op_id": f"noop-{self.term}-{self.node_id}"})
        self.noop_index = noop["index"]
        self.log_msg("*** Я ЛИДЕР *** (записал NOOP idx=%d)" % self.noop_index)
        term = self.term
        self.wake = {}
        for p in self.peers:
            ev = threading.Event()
            self.wake[p] = ev
            threading.Thread(target=self._replicator, args=(p, term, ev), daemon=True).start()
        self._advance_commit()   # для кластера из 1 узла и вообще на всякий случай

    def on_request_vote(self, msg):
        with self.lock:
            if msg["term"] < self.term:
                return {"type": "REQUEST_VOTE_REPLY", "term": self.term, "granted": False}
            if msg["term"] > self.term:
                self.term = msg["term"]
                self.voted_for = None
                if self.role != "follower":
                    self.role = "follower"
                    self.cond.notify_all()
                self._persist()
            up_to_date = (msg["last_log_term"], msg["last_log_index"]) >= \
                         (self.last_log_term(), self.last_log_index())
            granted = up_to_date and self.voted_for in (None, msg["candidate_id"])
            if granted:
                self.voted_for = msg["candidate_id"]
                self.last_heartbeat = time.time()
                self._persist()
            self.log_msg(f"REQUEST_VOTE от {msg['candidate_id']} (term {msg['term']}, "
                         f"его лог {msg['last_log_term']}/{msg['last_log_index']}): "
                         f"{'ГОЛОС ОТДАН' if granted else 'отказ'}")
            return {"type": "REQUEST_VOTE_REPLY", "term": self.term, "granted": granted}

    # ------------------------------------------------------------ репликация (лидер)
    def _replicator(self, peer, term, ev):
        """Поток на каждого follower на время лидерства: репликация + heartbeat."""
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
        """Один APPEND_ENTRIES. True, если был ответ и можно продолжать без паузы."""
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
                # откат: используем подсказку follower'а о конце его лога
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
            changed = False
            if msg["term"] > self.term:
                self.term = msg["term"]
                self.voted_for = None
                changed = True
            if self.role != "follower":
                self.log_msg(f"признаю лидера {msg['leader_id']}, становлюсь follower")
                self.cond.notify_all()
            self.role = "follower"
            self.leader_id = msg["leader_id"]
            self.last_heartbeat = time.time()
            if changed:
                self._persist()

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

            modified = False
            for k, e in enumerate(msg["entries"]):
                idx = prev_idx + 1 + k
                if idx <= len(self.log):
                    if self.log[idx - 1]["term"] == e["term"]:
                        continue                              # уже есть
                    self.log_msg(f"обрезаю хвост лога начиная с idx={idx} (конфликт term)")
                    del self.log[idx - 1:]                    # конфликт: обрезка + перезапись
                    modified = True
                self.log.append(e)
                modified = True
            if modified:
                self._rebuild_op_map()
                self._persist()
                if msg["entries"]:
                    self.log_msg(f"принял записи idx {prev_idx + 1}..{prev_idx + len(msg['entries'])}")

            match = prev_idx + len(msg["entries"])
            if msg["leader_commit"] > self.commit_index:
                self.commit_index = min(msg["leader_commit"], match)
                self.apply_committed()
            return reply(True, match)

    # ------------------------------------------------------------ клиентские операции
    def _not_leader(self):
        return {"type": "ERROR", "error": "not_leader", "leader": self.leader_id,
                "message": f"I am not leader, current leader is {self.leader_id}"}

    def handle_put(self, msg):
        key = msg.get("key")
        if key is None:
            return {"type": "ERROR", "error": "bad_request"}
        op_id = msg.get("op_id") or str(uuid.uuid4())
        with self.lock:
            if self.role != "leader":
                return self._not_leader()
            term = self.term
            idx = self.op_map.get(op_id)
            if idx is None:
                entry = self.append_command({"op": "PUT", "key": key, "value": msg.get("value"),
                                             "op_id": op_id})
                idx = entry["index"]
                self.log_msg(f"PUT {key}={msg.get('value')!r}: записано в лог idx={idx}, жду commit")
            else:
                self.log_msg(f"PUT retry op_id={op_id[:8]}: запись уже в логе idx={idx}")
            if self.crash_point == "before_replicate":
                self.log_msg("CRASH POINT before_replicate: убиваю процесс")
                os._exit(1)
            for ev in getattr(self, "wake", {}).values():
                ev.set()
            deadline = time.time() + PUT_TIMEOUT
            while True:
                if self.role != "leader" or self.term != term:
                    return {"type": "ERROR", "error": "leader_changed", "leader": self.leader_id,
                            "message": "leadership lost; outcome unknown, retry with same op_id"}
                if self.commit_index >= idx and self.last_applied >= idx:
                    if self.log[idx - 1]["command"].get("op_id") != op_id:
                        return {"type": "ERROR", "error": "leader_changed",
                                "message": "entry was overwritten, retry"}
                    if self.crash_point == "after_commit":
                        self.log_msg("CRASH POINT after_commit: убиваю процесс до ответа клиенту")
                        os._exit(1)
                    self.log_msg(f"PUT {key} подтверждён: idx={idx}")
                    return {"type": "PUT_OK", "key": key, "index": idx, "term": self.log[idx - 1]["term"]}
                remaining = deadline - time.time()
                if remaining <= 0:
                    return {"type": "ERROR", "error": "timeout", "leader": self.node_id,
                            "message": "no commit within timeout (no quorum?)"}
                self.cond.wait(remaining)

    def _confirm_leadership(self, term):
        """Пустой APPEND_ENTRIES большинству: убеждаемся, что мы ещё лидер (защита от stale read)."""
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
            while self.commit_index < self.noop_index:      # лидер ещё не закоммитил свой term
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
            found = key in self.state
            return {"type": "GET_OK", "key": key, "found": found,
                    "value": self.state.get(key), "commit_index": self.commit_index}

    def on_dump(self, msg):
        with self.lock:
            return {"type": "DUMP_OK", "node_id": self.node_id, "state": self.role,
                    "term": self.term, "leader": self.leader_id,
                    "log_len": len(self.log), "commit_index": self.commit_index,
                    "last_applied": self.last_applied, "data": dict(self.state),
                    "log_tail": self.log[-5:]}

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
    ap = argparse.ArgumentParser(description="Lab 4: узел кластера (упрощённый Raft)")
    ap.add_argument("node_id", type=int)
    ap.add_argument("--cluster", default=DEFAULT_CLUSTER, help="id:port,id:port,...")
    ap.add_argument("--data-dir", default="data")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    cluster = parse_cluster(args.cluster)
    if args.node_id not in cluster:
        raise SystemExit(f"ID {args.node_id} нет в --cluster")
    Node(args.node_id, cluster, args.data_dir).start()
