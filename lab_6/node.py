#!/usr/bin/env python3
"""
Lab 6. Snapshots, сжатие лога, наблюдаемость. Развитие Lab 5.

Запуск:
    python node.py 1 [--snapshot-threshold 50] [--metrics-interval 3]
    python node.py 2 --cluster 1:9001,2:9002,3:9003,4:9004,5:9005
    python node.py 1 --data-dir data          # каталог узла: data/node1/  (по умолчанию)
    python node.py 1 --data data/node1        # либо явный путь

Что добавлено относительно Lab 5:
  * Snapshot: когда len(log) >= SNAPSHOT_THRESHOLD, узел (любой, после apply) сохраняет
    snapshot.json = {last_included_index, last_included_term, state (KV), last_req (идемпотентность)}
    и только ПОСЛЕ его атомарной записи обрезает префикс лога (в памяти и в log.jsonl).
  * Лог в памяти теперь "виртуальный": self.log[0] имеет index = last_included_index + 1.
    Все обращения идут через last_log_index()/last_log_term()/term_at()/entry_at().
  * Recovery: snapshot -> state, last_req, last_applied=last_included_index; затем хвост лога
    доигрывается до commit_index (подсказка из commit.json).
  * Совместимость с AppendEntries (решение для prev_log_index < last_included_index):
    префикс внутри snapshot'а уже закоммичен, а закоммиченное одинаково у всех легитимных лидеров,
    поэтому такой запрос ПРИНИМАЕТСЯ: проверку prev_term пропускаем, записи с index <= snapshot
    пропускаем, остальные обрабатываем как обычно.
  * InstallSnapshot (бонус, "lite"): если next_index follower'а попал в уже сжатую часть лога,
    лидер шлёт INSTALL_SNAPSHOT целиком (state + last_req одним сообщением, без чанков).
    Follower ставит его атомарно: snapshot на диск -> обрезка лога -> замена state.
  * Метрики: раз в METRICS_INTERVAL секунд строка METRICS в stdout и в data/nodeN/metrics.log;
    на лидере - lag по каждому peer'у (last_log_index - match_index).

Сообщения (дополнительно к Lab 5):
    INSTALL_SNAPSHOT {term, leader_id, last_included_index, last_included_term, state, last_req}
                     -> INSTALL_SNAPSHOT_REPLY {term, success, match_index}
"""
import argparse
import copy
import json
import logging
import os
import random
import socket
import threading
import time

from storage import Storage

T0 = time.time()                       # момент старта процесса (для "время до готовности")

HEARTBEAT_INTERVAL = 0.5
ELECTION_TIMEOUT = (2.5, 4.5)
RPC_TIMEOUT = 1.0
PUT_TIMEOUT = 5.0
MAX_BATCH = 50
SNAPSHOT_THRESHOLD = 50                # длина лога (в памяти), после которой делаем snapshot
METRICS_INTERVAL_SEC = 3.0

DEFAULT_CLUSTER = "1:9001,2:9002,3:9003"


def parse_cluster(s):
    cluster = {}
    for item in s.split(","):
        nid, port = item.strip().split(":")
        cluster[int(nid)] = ("127.0.0.1", int(port))
    return cluster


class Node:
    def __init__(self, node_id, cluster, node_dir, snapshot_threshold=SNAPSHOT_THRESHOLD,
                 metrics_interval=METRICS_INTERVAL_SEC):
        self.node_id = node_id
        self.cluster = cluster
        self.peers = {i: a for i, a in cluster.items() if i != node_id}
        self.port = cluster[node_id][1]
        self.majority = len(cluster) // 2 + 1
        self.snapshot_threshold = snapshot_threshold
        self.metrics_interval = metrics_interval

        # --- snapshot (в памяти) ---
        self.snap_idx = 0              # last_included_index
        self.snap_term = 0             # last_included_term
        self.snap_data = {"state": {}, "last_req": {}}   # копия, неизменяемая после создания
        self.snapshots_taken = 0
        self.snapshots_installed = 0

        # --- volatile
        self.state = {}
        self.last_req = {}             # client_id -> {req_id, index, term, key}; меняется только в apply
        self.req_map = {}
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
        self.recovery_ms = 0.0
        self.ready_ms = None

        # --- persistent: загрузка с диска
        self.node_dir = node_dir
        t0 = time.time()
        self.storage = Storage(node_dir)
        saved = self.storage.load()
        self.term = saved["current_term"]
        self.voted_for = saved["voted_for"]
        self.log = saved["log"]                       # хвост: index = snap_idx+1, ...
        snap = saved["snapshot"]
        if snap:
            self.snap_idx = snap["last_included_index"]
            self.snap_term = snap["last_included_term"]
            self.snap_data = {"state": copy.deepcopy(snap["state"]),
                              "last_req": copy.deepcopy(snap["last_req"])}
            self.state = copy.deepcopy(snap["state"])
            self.last_req = copy.deepcopy(snap["last_req"])
            self.last_applied = self.snap_idx
        self.commit_index = saved["commit_index"]
        self._rebuild_req_map()
        self._recover(t0)

    # ------------------------------------------------------------ логирование / метрики
    def log_msg(self, text):
        logging.info("[node %d | term %d | %-9s | last_idx=%d commit=%d applied=%d snap=%d] %s",
                     self.node_id, self.term, self.role, self.last_log_index(),
                     self.commit_index, self.last_applied, self.snap_idx, text)

    def metrics_line(self):
        with self.lock:
            s = (f"METRICS node={self.node_id} role={self.role} term={self.term} "
                 f"log_len={len(self.log)} last_idx={self.last_log_index()} "
                 f"commit={self.commit_index} applied={self.last_applied} "
                 f"snap_idx={self.snap_idx} snap_term={self.snap_term} "
                 f"snapshots={self.snapshots_taken}+{self.snapshots_installed}inst "
                 f"keys={len(self.state)} fsyncs={self.storage.stats['fsyncs']} "
                 f"recovery_ms={self.recovery_ms}")
            if self.role == "leader" and self.match_index:
                last = self.last_log_index()
                lags = {p: last - self.match_index.get(p, 0) for p in sorted(self.peers)}
                s += f" max_lag={max(lags.values())} lag={lags}"
            return s

    def metrics_loop(self):
        path = os.path.join(self.node_dir, "metrics.log")
        while True:
            line = self.metrics_line()
            logging.info(line)
            try:
                with open(path, "a", encoding="utf-8") as f:
                    f.write(time.strftime("%H:%M:%S ") + line + "\n")
            except OSError:
                pass
            time.sleep(self.metrics_interval)

    def _recover(self, t0):
        """snapshot уже загружен в __init__; здесь доигрываем хвост лога до commit_index."""
        replay_from = self.last_applied
        self.apply_committed(quiet=True)
        self.recovery_ms = round((time.time() - t0) * 1000, 2)
        st = self.storage.stats
        fresh = (self.term == 0 and not self.log and self.snap_idx == 0)
        logging.info("[node %d] СТАРТ %s: data_dir=%s term=%d voted_for=%s snapshot_idx=%d "
                     "log_len=%d (хвост) last_idx=%d commit_index=%d applied=%d "
                     "(из snapshot: %d, доиграно из лога: %d) ключей=%d "
                     "RECOVERY=%.1f мс (чтение диска %.1f мс, отброшено байт хвоста: %d)",
                     self.node_id, "С НУЛЯ" if fresh else "С ДИСКА", self.node_dir, self.term,
                     self.voted_for, self.snap_idx, len(self.log), self.last_log_index(),
                     self.commit_index, self.last_applied, replay_from,
                     self.last_applied - replay_from, len(self.state), self.recovery_ms,
                     st["recovery_ms"], st["dropped_bytes"])
        if st["meta_corrupt"]:
            logging.warning("[node %d] meta.json повреждён - term/votedFor сброшены", self.node_id)
        if st["snapshot_corrupt"]:
            logging.warning("[node %d] snapshot.json повреждён - игнорирую", self.node_id)

    # ------------------------------------------------------------ сохранение
    def _save_meta(self):
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

    # ------------------------------------------------------------ лог с учётом snapshot
    def last_log_index(self):
        return self.snap_idx + len(self.log)

    def last_log_term(self):
        return self.log[-1]["term"] if self.log else self.snap_term

    def term_at(self, index):
        """term записи index; 0 для index=0; snap_term для index=snap_idx; None - если записи нет
        (внутри сжатого префикса или за концом лога)."""
        if index == 0:
            return 0
        if index == self.snap_idx:
            return self.snap_term
        if self.snap_idx < index <= self.last_log_index():
            return self.log[index - self.snap_idx - 1]["term"]
        return None

    def entry_at(self, index):
        if self.snap_idx < index <= self.last_log_index():
            return self.log[index - self.snap_idx - 1]
        return None

    def _rebuild_req_map(self):
        self.req_map = {}
        for e in self.log:
            c = e["command"]
            if c.get("client_id") is not None:
                self.req_map[(c["client_id"], c["req_id"])] = e["index"]

    def append_command(self, command):
        entry = {"term": self.term, "index": self.last_log_index() + 1, "command": command}
        self.log.append(entry)
        self.storage.append_entries([entry])
        if command.get("client_id") is not None:
            self.req_map[(command["client_id"], command["req_id"])] = entry["index"]
        return entry

    def apply_committed(self, quiet=False):
        """Под self.lock. last_req обновляется ТОЛЬКО здесь (и при установке snapshot'а)."""
        while self.last_applied < self.commit_index:
            entry = self.entry_at(self.last_applied + 1)
            if entry is None:
                self.log_msg(f"ОШИБКА: нет записи {self.last_applied + 1} для apply")
                break
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

    # ------------------------------------------------------------ snapshot
    def maybe_snapshot(self):
        """Под self.lock. Вызывается после продвижения last_applied."""
        if self.last_applied <= self.snap_idx or len(self.log) < self.snapshot_threshold:
            return
        idx = self.last_applied
        term = self.term_at(idx)
        if term is None:
            return
        old_len = len(self.log)
        data = {"state": copy.deepcopy(self.state), "last_req": copy.deepcopy(self.last_req)}
        # (1) snapshot на диск (атомарно, fsync) - ДО любых удалений
        self.storage.save_snapshot(idx, term, data["state"], data["last_req"])
        # (2) только теперь режем лог: в памяти и на диске
        self.log = self.log[idx - self.snap_idx:]
        self.storage.compact_log(self.log, idx)
        self.snap_idx, self.snap_term, self.snap_data = idx, term, data
        self.snapshots_taken += 1
        self._rebuild_req_map()
        self.log_msg(f"*** SNAPSHOT *** last_included_index={idx} last_included_term={term} "
                     f"ключей={len(data['state'])} log_len {old_len} -> {len(self.log)}")

    def _after_apply(self):
        self.apply_committed()
        self.maybe_snapshot()
        self.cond.notify_all()

    def on_install_snapshot(self, msg):
        with self.lock:
            def reply(success, match=0):
                return {"type": "INSTALL_SNAPSHOT_REPLY", "term": self.term, "from_id": self.node_id,
                        "success": success, "match_index": match}

            if msg["term"] < self.term:
                return reply(False)
            if msg["term"] > self.term:
                self.term = msg["term"]
                self.voted_for = None
                self._save_meta()
            if self.role != "follower":
                self.cond.notify_all()
            self.role = "follower"
            self.leader_id = msg["leader_id"]
            self.last_heartbeat = time.time()

            idx, sterm = msg["last_included_index"], msg["last_included_term"]
            if idx <= self.commit_index:
                # у нас уже закоммичено не меньше - состояние совпадает с лидерским до idx
                return reply(True, idx)

            # хвост лога сохраняем, только если запись idx у нас есть и с тем же term
            if idx <= self.last_log_index() and self.term_at(idx) == sterm:
                remaining = self.log[idx - self.snap_idx:]
            else:
                remaining = []
            state, last_req = copy.deepcopy(msg["state"]), copy.deepcopy(msg["last_req"])
            self.storage.save_snapshot(idx, sterm, state, last_req)       # (1) snapshot на диск
            self.log = remaining                                          # (2) обрезка лога
            self.storage.compact_log(self.log, idx)
            self.snap_idx, self.snap_term = idx, sterm
            self.snap_data = {"state": copy.deepcopy(state), "last_req": copy.deepcopy(last_req)}
            self.state, self.last_req = state, last_req
            self.commit_index = idx
            self._save_commit()
            self.last_applied = idx
            self.snapshots_installed += 1
            self._rebuild_req_map()
            self.log_msg(f"*** INSTALL SNAPSHOT *** от лидера {msg['leader_id']}: "
                         f"last_included_index={idx} term={sterm}, хвост лога сохранён: {len(remaining)}")
            self._after_apply()
            return reply(True, idx)

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
            self._save_meta()
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
        self.next_index = {p: self.last_log_index() + 1 for p in self.peers}
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
                self._save_meta()
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
                    more = self.next_index.get(peer, 1) <= self.last_log_index()
                if more:
                    continue
            ev.wait(HEARTBEAT_INTERVAL)
            ev.clear()

    def _replicate_to(self, peer, term):
        with self.lock:
            if self.role != "leader" or self.term != term:
                return False
            ni = self.next_index[peer]
            if ni <= self.snap_idx:
                # нужный follower'у префикс уже сжат - шлём snapshot целиком
                msg = {"type": "INSTALL_SNAPSHOT", "term": term, "leader_id": self.node_id,
                       "last_included_index": self.snap_idx, "last_included_term": self.snap_term,
                       "state": self.snap_data["state"], "last_req": self.snap_data["last_req"]}
                installing = True
            else:
                installing = False
                prev_idx = ni - 1
                prev_term = self.term_at(prev_idx)
                entries = self.log[ni - self.snap_idx - 1: ni - self.snap_idx - 1 + MAX_BATCH]
                msg = {"type": "APPEND_ENTRIES", "term": term, "leader_id": self.node_id,
                       "prev_log_index": prev_idx, "prev_log_term": prev_term,
                       "entries": entries, "leader_commit": self.commit_index}
        if installing:
            self.log_msg(f"узлу {peer} нужен префикс <= {msg['last_included_index']} (next_index={ni}): "
                         f"отправляю INSTALL_SNAPSHOT")
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
            elif not installing:
                hint = r.get("last_log_index", ni - 2)
                self.next_index[peer] = max(1, min(ni - 1, hint + 1))
                self.log_msg(f"узел {peer}: лог не совпал на prev={prev_idx}, "
                             f"next_index -> {self.next_index[peer]}")
        return True

    def _advance_commit(self):
        """Под self.lock. N коммитится, если он на большинстве И из текущего term."""
        for n in range(self.last_log_index(), self.commit_index, -1):
            if self.term_at(n) != self.term:
                break
            count = 1 + sum(1 for m in self.match_index.values() if m >= n)
            if count >= self.majority:
                self.commit_index = n
                self._save_commit()
                self.log_msg(f"COMMIT idx={n} ({count}/{len(self.cluster)})")
                break
        self._after_apply()

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
            if prev_idx > self.last_log_index():
                return reply(False, hint=self.last_log_index())
            if prev_idx >= self.snap_idx:
                # prev_idx == snap_idx сравнивается со snap_term (виртуальный префикс)
                have = self.term_at(prev_idx)
                if have != prev_term:
                    i = prev_idx
                    while i > self.snap_idx + 1 and self.term_at(i - 1) == have:
                        i -= 1
                    self.log_msg(f"конфликт на prev_idx={prev_idx} (term {have} != {prev_term})")
                    return reply(False, hint=i - 1)
            # else: prev_idx < snap_idx - префикс внутри snapshot'а закоммичен => согласован

            new, truncated = [], False
            for k, e in enumerate(msg["entries"]):
                idx = prev_idx + 1 + k
                if idx <= self.snap_idx:
                    continue                                   # уже внутри snapshot'а
                if idx <= self.last_log_index():
                    if self.term_at(idx) == e["term"]:
                        continue
                    if idx <= self.commit_index:
                        self.log_msg(f"ОШИБКА: конфликт в закоммиченной записи idx={idx}, отказ")
                        return reply(False)
                    self.log_msg(f"обрезаю хвост лога начиная с idx={idx} (конфликт term)")
                    del self.log[idx - self.snap_idx - 1:]
                    self.storage.truncate_from(idx)
                    truncated = True
                self.log.append(e)
                new.append(e)
            if new:
                self.storage.append_entries(new)
                self.log_msg(f"принял записи idx {new[0]['index']}..{new[-1]['index']} (на диске)")
            if truncated:
                self._rebuild_req_map()
            else:
                for e in new:
                    c = e["command"]
                    if c.get("client_id") is not None:
                        self.req_map[(c["client_id"], c["req_id"])] = e["index"]

            match = prev_idx + len(msg["entries"])
            if msg["leader_commit"] > self.commit_index:
                self.commit_index = max(self.commit_index, min(msg["leader_commit"], match))
                self._save_commit()
                self._after_apply()
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

            sess = self.last_req.get(cid)            # last_req переживает snapshot и рестарт
            if sess and rid == sess["req_id"]:
                self.log_msg(f"PUT ({cid}, {rid}): повтор, уже применён на idx={sess['index']} "
                             f"- возвращаю прежний результат")
                return self._put_ok(sess, True)
            if sess and rid < sess["req_id"]:
                return {"type": "ERROR", "error": "stale_request",
                        "message": f"req_id {rid} старше последнего обработанного {sess['req_id']}"}

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
            self._advance_commit()                   # кластер из одного узла коммитит сам

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
                   "log_len": len(self.log), "last_log_index": self.last_log_index(),
                   "snap_idx": self.snap_idx, "snap_term": self.snap_term,
                   "commit_index": self.commit_index, "last_applied": self.last_applied,
                   "data": dict(self.state),
                   "last_req": {c: s["req_id"] for c, s in self.last_req.items()},
                   "snapshots_taken": self.snapshots_taken,
                   "snapshots_installed": self.snapshots_installed,
                   "recovery_ms": self.recovery_ms, "ready_ms": self.ready_ms,
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
            "INSTALL_SNAPSHOT": self.on_install_snapshot,
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
        self.ready_ms = round((time.time() - T0) * 1000, 1)
        self.log_msg(f"ГОТОВ принимать запросы: {self.ready_ms} мс от старта процесса "
                     f"(из них recovery {self.recovery_ms} мс), порт {self.port}, "
                     f"peers: {sorted(self.peers)}")
        while True:
            conn, addr = srv.accept()
            threading.Thread(target=self._handle_conn, args=(conn, addr), daemon=True).start()

    def start(self):
        threading.Thread(target=self.serve, daemon=True).start()
        threading.Thread(target=self.election_timeout_loop, daemon=True).start()
        threading.Thread(target=self.metrics_loop, daemon=True).start()
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            self.log_msg("остановлен (Ctrl+C)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Lab 6: узел кластера (Raft + snapshots + метрики)")
    ap.add_argument("node_id", type=int)
    ap.add_argument("--cluster", default=DEFAULT_CLUSTER, help="id:port,id:port,...")
    ap.add_argument("--data-dir", default="data", help="корень; каталог узла = <root>/node<id>")
    ap.add_argument("--data", default=None, help="явный каталог узла (приоритетнее --data-dir)")
    ap.add_argument("--snapshot-threshold", type=int, default=SNAPSHOT_THRESHOLD,
                    help="сжимать лог при len(log) >= N (очень большое N = snapshot'ы выключены)")
    ap.add_argument("--metrics-interval", type=float, default=METRICS_INTERVAL_SEC)
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    cluster = parse_cluster(args.cluster)
    if args.node_id not in cluster:
        raise SystemExit(f"ID {args.node_id} нет в --cluster")
    node_dir = args.data or os.path.join(args.data_dir, f"node{args.node_id}")
    Node(args.node_id, cluster, node_dir, args.snapshot_threshold, args.metrics_interval).start()
