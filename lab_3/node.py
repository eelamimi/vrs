#!/usr/bin/env python3
"""
Lab 3. Выборы лидера (упрощённый Bully + term) и Primary-Backup репликация.

Запуск (в отдельных терминалах):
    python node.py 1
    python node.py 2
    python node.py 3

Кластер по умолчанию: 1:9001, 2:9002, 3:9003. Можно задать свой:
    python node.py 2 --cluster 1:9001,2:9002,3:9003,4:9004,5:9005

Протокол: одна строка JSON на запрос, одна строка JSON в ответ (TCP).

Сообщения между узлами:
    HEARTBEAT   {leader_id, term, last_ver}        -> HEARTBEAT_ACK | STALE
    ELECTION    {from_id, term}                    -> ELECTION_OK
    COORDINATOR {leader_id, term}                  -> COORDINATOR_ACK | STALE
    REPLICATE   {key, value, ver, op_id, leader_id, term} -> REPLICATE_ACK | ERROR(stale_term)
    SYNC        {}                                 -> SNAPSHOT {data, term, leader, last_ver}
Сообщения клиента:
    PUT {key, value} -> PUT_OK | ERROR(not_leader / no_quorum)
    GET {key}        -> GET_OK | ERROR(not_leader)
    DUMP {}          -> DUMP_OK (локальное состояние узла: для отладки/проверки репликации)

Принятые решения (фиксируем для отчёта):
  * Алгоритм выборов: Bully. ELECTION шлётся только узлам с БОЛЬШИМ ID.
    Если кто-то ответил ELECTION_OK -> ждём COORDINATOR; если никто -> становимся лидером.
  * term увеличивается при каждых выборах; сообщения с меньшим term отвергаются (STALE).
  * Политика репликации: MAJORITY от размера всего кластера (включая лидера).
    PUT_OK отдаётся только после ACK от большинства.
  * Версия записи ver = (term, seq). Побеждает большая версия. Благодаря этому
    вернувшийся узел с пустыми данными не затирает данные (перед тем как стать
    лидером, узел подтягивает SNAPn SHOT от живых peers).
  * Persistent storage: data и term сохраняются в JSON-файл на диск.
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

# ---------------------------------------------------------------- таймауты
HEARTBEAT_INTERVAL = 1.0          # лидер шлёт heartbeat раз в секунду
ELECTION_TIMEOUT = (3.0, 5.0)     # follower ждёт heartbeat 3-5 с (рандом, чтобы не стартовать одновременно)
RPC_TIMEOUT = 1.0                 # таймаут одного обращения к peer
COORDINATOR_WAIT = 3.0            # сколько ждём COORDINATOR после ELECTION_OK

DEFAULT_CLUSTER = "1:9001,2:9002,3:9003"


def parse_cluster(s):
    """'1:9001,2:9002' -> {1: ('127.0.0.1', 9001), 2: (...)}"""
    cluster = {}
    for item in s.split(","):
        nid, port = item.strip().split(":")
        cluster[int(nid)] = ("127.0.0.1", int(port))
    return cluster


class Node:
    def __init__(self, node_id, cluster, data_dir="data"):
        self.node_id = node_id
        self.cluster = cluster                                   # {id: (host, port)}
        self.peers = {i: a for i, a in cluster.items() if i != node_id}
        self.port = cluster[node_id][1]

        self.state = "follower"        # follower | candidate | leader
        self.current_leader = None
        self.term = 0
        self.data = {}                 # key -> {"value": ..., "ver": (term, seq)}
        self.seq = 0                   # счётчик записей текущего лидера

        self.lock = threading.RLock()  # защищает state/term/data/current_leader
        self.put_lock = threading.Lock()  # PUT'ы на лидере выполняются по одному
        self.electing = False
        self.last_heartbeat = time.time()
        self.election_timeout = random.uniform(*ELECTION_TIMEOUT)

        os.makedirs(data_dir, exist_ok=True)
        self.path = os.path.join(data_dir, f"node{node_id}.json")
        self._load()

    # ------------------------------------------------------------ логирование
    def log(self, text):
        logging.info("[node %d | term %d | %-9s] %s", self.node_id, self.term, self.state, text)

    # ------------------------------------------------------------ persistence
    def _load(self):
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path, encoding="utf-8") as f:
                saved = json.load(f)
            self.term = saved.get("term", 0)
            self.data = {k: {"value": v["value"], "ver": tuple(v["ver"])}
                         for k, v in saved.get("data", {}).items()}
            logging.info("[node %d] загружено с диска: term=%d, ключей=%d",
                         self.node_id, self.term, len(self.data))
        except (OSError, ValueError) as e:
            logging.warning("[node %d] не удалось прочитать %s: %s", self.node_id, self.path, e)

    def _persist(self):
        """Вызывать под self.lock. Атомарная запись: tmp-файл + rename."""
        tmp = self.path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"term": self.term, "data": self.data}, f)
            os.replace(tmp, self.path)
        except OSError as e:
            logging.warning("[node %d] ошибка записи на диск: %s", self.node_id, e)

    # ------------------------------------------------------------ сеть
    def send(self, peer_id, msg, timeout=RPC_TIMEOUT):
        """Отправить JSON peer'у и прочитать ответ. None при любой сетевой ошибке."""
        host, port = self.cluster[peer_id]
        try:
            with socket.create_connection((host, port), timeout=timeout) as s:
                s.settimeout(timeout)
                s.sendall((json.dumps(msg) + "\n").encode("utf-8"))
                line = s.makefile("r", encoding="utf-8").readline()
            return json.loads(line) if line else None
        except (OSError, ValueError):
            return None

    def broadcast(self, msg, targets=None):
        """Параллельно отправить msg всем targets (по умолчанию - всем peers).
        Возвращает {peer_id: ответ или None}."""
        targets = list(self.peers if targets is None else targets)
        results = {}

        def worker(pid):
            results[pid] = self.send(pid, msg)

        threads = [threading.Thread(target=worker, args=(pid,), daemon=True) for pid in targets]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return results

    # ------------------------------------------------------------ данные
    def last_ver(self):
        with self.lock:
            return max((v["ver"] for v in self.data.values()), default=(0, 0))

    def apply(self, key, value, ver):
        """Применить запись, только если её версия новее имеющейся."""
        with self.lock:
            cur = self.data.get(key)
            if cur is None or tuple(ver) > cur["ver"]:
                self.data[key] = {"value": value, "ver": tuple(ver)}
                self._persist()

    def snapshot(self):
        with self.lock:
            return {k: {"value": v["value"], "ver": list(v["ver"])} for k, v in self.data.items()}

    def merge_snapshot(self, snap):
        for k, v in snap.items():
            self.apply(k, v["value"], tuple(v["ver"]))

    def sync_from_peers(self, only=None):
        """Подтянуть SNAPSHOT от peers: слить данные, взять максимальный term."""
        replies = self.broadcast({"type": "SYNC"}, targets=only)
        leader_hint = None
        for pid, r in replies.items():
            if r and r.get("type") == "SNAPSHOT":
                self.merge_snapshot(r["data"])
                with self.lock:
                    if r["term"] > self.term:
                        self.term = r["term"]
                        self._persist()
                if r.get("leader") == pid:
                    leader_hint = pid
        return leader_hint

    # ------------------------------------------------------------ term / роли
    def _step_down(self, term, leader=None):
        with self.lock:
            if term > self.term:
                self.term = term
            was = self.state
            self.state = "follower"
            self.current_leader = leader
            self.last_heartbeat = time.time()
            self._persist()
        if was != "follower":
            self.log(f"перешёл в follower (новый лидер: {leader})")

    def _accept_leader(self, leader_id, term):
        """Общая логика для HEARTBEAT / COORDINATOR / REPLICATE.
        Возвращает dict-ответ STALE, если сообщение нужно отвергнуть, иначе None."""
        with self.lock:
            if term < self.term:
                return {"type": "STALE", "from_id": self.node_id,
                        "term": self.term, "leader": self.current_leader}
            if term == self.term and self.state == "leader" and leader_id != self.node_id:
                # два лидера в одном term (гонка выборов): побеждает больший ID
                if leader_id < self.node_id:
                    self.term += 1
                    self._persist()
                    self.log(f"конфликт лидеров с {leader_id}, повышаю term")
                    return {"type": "STALE", "from_id": self.node_id,
                            "term": self.term, "leader": self.node_id}
            was_state, was_leader = self.state, self.current_leader
            if term > self.term:
                self.term = term
                self._persist()
            self.state = "follower"
            self.current_leader = leader_id
            self.last_heartbeat = time.time()
        if was_state != "follower" or was_leader != leader_id:
            self.log(f"признаю лидера {leader_id}")
        return None

    def _process_replies(self, replies):
        """Лидер смотрит на ответы: если peer живёт в более новом term - мы устарели."""
        for r in replies.values():
            if not r:
                continue
            rt = r.get("term", 0)
            with self.lock:
                if self.state != "leader" or rt <= self.term:
                    continue
                known_leader = r.get("leader")
            if known_leader in (None, self.node_id):
                # peer просто накрутил term неудачными выборами - подтягиваем term
                with self.lock:
                    self.term = rt
                    self._persist()
            else:
                self.log(f"вижу term {rt} и лидера {known_leader} - я устарел")
                self._step_down(rt, known_leader)

    # ------------------------------------------------------------ выборы
    def start_election(self):
        with self.lock:
            if self.electing or self.state == "leader":
                return
            self.electing = True
            self.state = "candidate"
            self.term += 1
            my_term = self.term
            self._persist()
        try:
            self.log("начинаю выборы")
            higher = [i for i in self.peers if i > self.node_id]
            replies = self.broadcast({"type": "ELECTION", "from_id": self.node_id,
                                      "term": my_term}, targets=higher)
            got_ok = False
            for r in replies.values():
                if r and r.get("type") == "ELECTION_OK":
                    got_ok = True
                    with self.lock:
                        if r.get("term", 0) > self.term:
                            self.term = r["term"]
            with self.lock:
                if self.state != "candidate":      # за это время объявился чужой лидер
                    return
            if got_ok:
                self.log("есть живой узел с большим ID - жду COORDINATOR")
                deadline = time.time() + COORDINATOR_WAIT
                while time.time() < deadline:
                    with self.lock:
                        if self.state != "candidate":
                            return
                    time.sleep(0.1)
                self.log("COORDINATOR не пришёл - повторю выборы")
                with self.lock:
                    self.state = "follower"
                return
            # никто сильнее не ответил: сначала актуализируем данные, затем лидерство
            self.sync_from_peers()
            with self.lock:
                if self.state != "candidate":
                    return
            self.become_leader()
        finally:
            with self.lock:
                self.electing = False
                if self.state == "candidate":
                    self.state = "follower"

    def become_leader(self):
        with self.lock:
            self.state = "leader"
            self.current_leader = self.node_id
            self.seq = 0
            term = self.term
        self.log("*** Я ЛИДЕР ***")
        replies = self.broadcast({"type": "COORDINATOR", "leader_id": self.node_id, "term": term})
        self._process_replies(replies)

    def _announce_to(self, peer_id):
        with self.lock:
            if self.state != "leader":
                return
            msg = {"type": "COORDINATOR", "leader_id": self.node_id, "term": self.term}
        self._process_replies({peer_id: self.send(peer_id, msg)})

    # ------------------------------------------------------------ фоновые циклы
    def heartbeat_loop(self):
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            with self.lock:
                if self.state != "leader":
                    continue
                msg = {"type": "HEARTBEAT", "leader_id": self.node_id,
                       "term": self.term, "last_ver": list(self.last_ver())}
            self._process_replies(self.broadcast(msg))

    def election_timeout_loop(self):
        while True:
            time.sleep(0.2)
            with self.lock:
                if self.state == "leader" or self.electing:
                    continue
                expired = time.time() - self.last_heartbeat > self.election_timeout
            if expired:
                self.log("heartbeat потерян - выборы")
                self.election_timeout = random.uniform(*ELECTION_TIMEOUT)
                self.start_election()

    # ------------------------------------------------------------ обработка сообщений
    def handle_message(self, msg, addr=None):
        handlers = {
            "HEARTBEAT": self.on_heartbeat,
            "ELECTION": self.on_election,
            "COORDINATOR": self.on_coordinator,
            "REPLICATE": self.on_replicate,
            "SYNC": self.on_sync,
            "PUT": lambda m: self.handle_client_put(m.get("key"), m.get("value")),
            "GET": self.on_get,
            "DUMP": self.on_dump,
        }
        handler = handlers.get(msg.get("type"))
        if handler is None:
            return {"type": "ERROR", "error": "unknown_type"}
        return handler(msg)

    def on_heartbeat(self, msg):
        stale = self._accept_leader(msg["leader_id"], msg["term"])
        if stale:
            return stale
        # если мы отстали по данным - подтягиваем snapshot с лидера
        if tuple(msg.get("last_ver", (0, 0))) > self.last_ver():
            threading.Thread(target=self.sync_from_peers, args=([msg["leader_id"]],),
                             daemon=True).start()
        return {"type": "HEARTBEAT_ACK", "from_id": self.node_id, "term": self.term}

    def on_election(self, msg):
        frm = msg["from_id"]
        if frm > self.node_id:
            return {"type": "ERROR", "error": "election_from_stronger_node"}
        with self.lock:
            if msg["term"] > self.term:
                self.term = msg["term"]
                self._persist()
            am_leader = self.state == "leader"
            term = self.term
        if am_leader:
            # я уже лидер - просто сообщаю об этом инициатору
            threading.Thread(target=self._announce_to, args=(frm,), daemon=True).start()
        else:
            # я сильнее инициатора - беру выборы на себя
            threading.Thread(target=self.start_election, daemon=True).start()
        return {"type": "ELECTION_OK", "from_id": self.node_id, "term": term}

    def on_coordinator(self, msg):
        stale = self._accept_leader(msg["leader_id"], msg["term"])
        if stale:
            return stale
        threading.Thread(target=self.sync_from_peers, args=([msg["leader_id"]],),
                         daemon=True).start()
        return {"type": "COORDINATOR_ACK", "from_id": self.node_id, "term": self.term}

    def on_replicate(self, msg):
        stale = self._accept_leader(msg["leader_id"], msg["term"])
        if stale:
            return {"type": "ERROR", "error": "stale_term", **{k: stale[k] for k in ("term", "leader")}}
        self.apply(msg["key"], msg["value"], tuple(msg["ver"]))
        return {"type": "REPLICATE_ACK", "op_id": msg["op_id"],
                "from_id": self.node_id, "term": self.term}

    def on_sync(self, msg):
        with self.lock:
            return {"type": "SNAPSHOT", "from_id": self.node_id, "term": self.term,
                    "leader": self.current_leader, "data": self.snapshot(),
                    "last_ver": list(self.last_ver())}

    def on_dump(self, msg):
        with self.lock:
            return {"type": "DUMP_OK", "node_id": self.node_id, "state": self.state,
                    "term": self.term, "leader": self.current_leader,
                    "data": {k: v["value"] for k, v in self.data.items()}}

    # ------------------------------------------------------------ клиентские операции
    def _not_leader(self):
        with self.lock:
            leader = self.current_leader
        return {"type": "ERROR", "error": "not_leader", "leader": leader,
                "message": f"I am not leader, current leader is {leader}"}

    def on_get(self, msg):
        with self.lock:
            if self.state != "leader":
                return self._not_leader()
            entry = self.data.get(msg["key"])
        return {"type": "GET_OK", "key": msg["key"], "found": entry is not None,
                "value": entry["value"] if entry else None}

    def handle_client_put(self, key, value):
        if key is None:
            return {"type": "ERROR", "error": "bad_request"}
        with self.lock:
            if self.state != "leader":
                return self._not_leader()
        with self.put_lock:                      # записи упорядочены
            with self.lock:
                if self.state != "leader":
                    return self._not_leader()
                self.seq += 1
                ver = (self.term, self.seq)
                term = self.term
            op_id = str(uuid.uuid4())
            replies = self.broadcast({"type": "REPLICATE", "key": key, "value": value,
                                      "ver": list(ver), "op_id": op_id,
                                      "leader_id": self.node_id, "term": term})
            self._process_replies(replies)
            acks = 1 + sum(1 for r in replies.values()
                           if r and r.get("type") == "REPLICATE_ACK" and r.get("op_id") == op_id)
            need = len(self.cluster) // 2 + 1
            with self.lock:
                still_leader = self.state == "leader" and self.term >= term
            if still_leader and acks >= need:
                self.apply(key, value, ver)
                self.log(f"PUT {key}={value!r} подтверждён ({acks}/{len(self.cluster)}, нужно {need})")
                return {"type": "PUT_OK", "key": key, "acks": acks}
            self.log(f"PUT {key} НЕ подтверждён: acks={acks}, нужно {need}")
            return {"type": "ERROR", "error": "no_quorum", "acks": acks, "need": need}

    # ------------------------------------------------------------ запуск
    def _handle_conn(self, conn, addr):
        with conn:
            conn.settimeout(5)
            try:
                line = conn.makefile("r", encoding="utf-8").readline()
                if not line:
                    return
                reply = self.handle_message(json.loads(line), addr)
            except Exception as e:  # noqa: BLE001 - узел не должен падать от плохого сообщения
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
        self.log(f"слушаю порт {self.port}, peers: {sorted(self.peers)}")
        while True:
            conn, addr = srv.accept()
            threading.Thread(target=self._handle_conn, args=(conn, addr), daemon=True).start()

    def _startup(self):
        """Старт: узнаём актуальный term/данные у живых peers, затем Bully-выборы
        (вернувшийся узел с большим ID по алгоритму Bully забирает лидерство)."""
        time.sleep(0.5)
        self.sync_from_peers()
        with self.lock:
            self.last_heartbeat = time.time()
        self.start_election()

    def start(self):
        threading.Thread(target=self.serve, daemon=True).start()
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
        threading.Thread(target=self.election_timeout_loop, daemon=True).start()
        threading.Thread(target=self._startup, daemon=True).start()
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            self.log("остановлен (Ctrl+C)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Lab 3: узел кластера (Bully + primary-backup)")
    ap.add_argument("node_id", type=int)
    ap.add_argument("--cluster", default=DEFAULT_CLUSTER, help="id:port,id:port,...")
    ap.add_argument("--data-dir", default="data")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    cluster = parse_cluster(args.cluster)
    if args.node_id not in cluster:
        raise SystemExit(f"ID {args.node_id} нет в --cluster")
    Node(args.node_id, cluster, args.data_dir).start()
