"""
Слой хранения для Lab 6 (развитие Lab 5: добавлены snapshot и сжатие лога).

Каталог узла (data/node1/):
    meta.json      {"current_term", "voted_for"}  - атомарная замена + fsync
    log.jsonl      append-only лог; теперь может НАЧИНАТЬСЯ НЕ С ИНДЕКСА 1 (после сжатия)
                       <crc32 hex>\t<json записи>\n
    commit.json    {"commit_index"} - подсказка (без fsync, см. Lab 5)
    snapshot.json  {"last_included_index", "last_included_term", "state", "last_req"}
                   атомарная запись: tmp -> fsync -> self._atomic_replace -> fsync(каталога)
    metrics.log    строки METRICS (пишет узел, не Storage)

Инвариант сжатия лога (порядок операций ВАЖЕН):
    1) save_snapshot(N)      - snapshot на диске (fsync) ...
    2) compact_log(tail, N)  - ... и только потом из log.jsonl удаляется префикс <= N.
Если процесс умер между 1) и 2), на диске лежит snapshot(N) и "длинный" лог. load() это
обнаруживает (лог начинается с индекса <= N) и достраивает шаг 2). Обратная ситуация
(лог уже обрезан, а snapshot'а нет) невозможна при нормальной работе: в этом случае
load() бросает StorageError, а не молча теряет данные.

base = индекс записи, ПРЕДШЕСТВУЮЩЕЙ первой записи в log.jsonl (= last_included_index
snapshot'а при обрезанном логе, 0 - если лог цел). Запись с индексом i лежит в offsets[i-base-1].
"""
import json
import os
import time
import zlib


class StorageError(Exception):
    pass


def _fsync_dir(path):
    try:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass


def _encode(entry):
    body = json.dumps(entry, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return b"%08x\t" % (zlib.crc32(body) & 0xFFFFFFFF) + body + b"\n"


class Storage:
    def __init__(self, data_dir):
        self.dir = data_dir
        os.makedirs(self.dir, exist_ok=True)
        self.meta_path = os.path.join(self.dir, "meta.json")
        self.log_path = os.path.join(self.dir, "log.jsonl")
        self.commit_path = os.path.join(self.dir, "commit.json")
        self.snap_path = os.path.join(self.dir, "snapshot.json")
        self._f = None
        self.base = 0
        self.offsets = []
        self.stats = {"meta_writes": 0, "log_entries_written": 0, "fsyncs": 0,
                      "truncations": 0, "snapshots_written": 0, "compactions": 0,
                      "recovery_ms": 0.0, "dropped_bytes": 0,
                      "meta_corrupt": False, "snapshot_corrupt": False}

    # ------------------------------------------------------------ чтение
    def load(self):
        """-> {'current_term','voted_for','commit_index','log','snapshot'}.
        snapshot: None или {'last_included_index','last_included_term','state','last_req'}.
        Элементы log имеют индексы snapshot.last_included_index+1, ... (лог без дыр)."""
        t0 = time.time()
        term, voted_for, commit = 0, None, 0

        if os.path.exists(self.meta_path):
            try:
                with open(self.meta_path, encoding="utf-8") as f:
                    m = json.load(f)
                term = int(m["current_term"])
                voted_for = m["voted_for"]
                if voted_for is not None:
                    voted_for = int(voted_for)
            except (OSError, ValueError, KeyError, TypeError):
                term, voted_for = 0, None
                self.stats["meta_corrupt"] = True

        snap = self.load_snapshot()
        snap_idx = snap["last_included_index"] if snap else 0

        log = self._load_log()
        last = self.base + len(log)
        if snap_idx >= last:
            # весь лог покрыт snapshot'ом (или лог пуст): сбрасываем лог, base = snap_idx
            if log or self.base != snap_idx:
                self.rewrite_log([], snap_idx)
            log = []
        elif snap_idx > self.base:
            # лог не успели обрезать после snapshot'а (крах между шагами 1 и 2) - достраиваем
            log = log[snap_idx - self.base:]
            self.rewrite_log(log, snap_idx)
        elif snap_idx < self.base:
            raise StorageError(
                f"{self.log_path}: лог начинается с индекса {self.base + 1}, а snapshot покрывает "
                f"только до {snap_idx} - snapshot.json потерян или повреждён")

        if os.path.exists(self.commit_path):
            try:
                with open(self.commit_path, encoding="utf-8") as f:
                    commit = int(json.load(f)["commit_index"])
            except (OSError, ValueError, KeyError, TypeError):
                commit = 0
        commit = max(snap_idx, min(commit, self.base + len(log)))

        self.stats["recovery_ms"] = round((time.time() - t0) * 1000, 2)
        return {"current_term": term, "voted_for": voted_for, "commit_index": commit,
                "log": log, "snapshot": snap}

    def load_snapshot(self):
        if not os.path.exists(self.snap_path):
            return None
        try:
            with open(self.snap_path, encoding="utf-8") as f:
                s = json.load(f)
            snap = {"last_included_index": int(s["last_included_index"]),
                    "last_included_term": int(s["last_included_term"]),
                    "state": dict(s["state"]), "last_req": dict(s["last_req"])}
            if snap["last_included_index"] < 1:
                raise ValueError("bad index")
            return snap
        except (OSError, ValueError, KeyError, TypeError):
            self.stats["snapshot_corrupt"] = True
            return None

    @staticmethod
    def _atomic_replace(tmp, dst):
        last_err = None
        for _ in range(40):
            try:
                os.replace(tmp, dst)
                return
            except PermissionError as e:
                last_err = e
                time.sleep(0.05)
        raise last_err

    def _load_log(self):
        log, self.offsets, self.base = [], [], 0
        good_end = 0
        if os.path.exists(self.log_path):
            with open(self.log_path, "rb") as f:
                raw = f.read()
            pos = 0
            while pos < len(raw):
                nl = raw.find(b"\n", pos)
                if nl == -1:
                    break                                   # оборванная последняя запись
                expected = (log[-1]["index"] + 1) if log else None
                entry = self._decode(raw[pos:nl], expected)
                if entry is None:
                    break                                   # повреждение: всё дальше недостоверно
                if not log:
                    self.base = entry["index"] - 1
                self.offsets.append(pos)
                log.append(entry)
                pos = nl + 1
                good_end = pos
            if good_end < len(raw):
                self.stats["dropped_bytes"] = len(raw) - good_end
                with open(self.log_path, "r+b") as f:
                    f.truncate(good_end)
                    f.flush()
                    os.fsync(f.fileno())
        else:
            open(self.log_path, "wb").close()
            _fsync_dir(self.dir)
        self._open()
        return log

    @staticmethod
    def _decode(line, expected_index):
        """expected_index=None - для первой записи файла (любой индекс >= 1)."""
        try:
            crc_hex, body = line.split(b"\t", 1)
            if int(crc_hex, 16) != (zlib.crc32(body) & 0xFFFFFFFF):
                return None
            e = json.loads(body.decode("utf-8"))
            if "term" not in e or "command" not in e or not isinstance(e["index"], int):
                return None
            if expected_index is None:
                if e["index"] < 1:
                    return None
            elif e["index"] != expected_index:
                return None
            return e
        except (ValueError, KeyError, TypeError, UnicodeDecodeError):
            return None

    def _open(self):
        if self._f:
            self._f.close()
        self._f = open(self.log_path, "r+b")
        self._f.seek(0, os.SEEK_END)

    def _sync(self):
        self._f.flush()
        os.fsync(self._f.fileno())
        self.stats["fsyncs"] += 1

    # ------------------------------------------------------------ hard state / commit
    def save_hard_state(self, current_term, voted_for):
        tmp = self.meta_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"current_term": current_term, "voted_for": voted_for}, f)
            f.flush()
            os.fsync(f.fileno())
        self._atomic_replace(tmp, self.meta_path)
        _fsync_dir(self.dir)
        self.stats["meta_writes"] += 1
        self.stats["fsyncs"] += 1

    def save_commit(self, commit_index):
        tmp = self.commit_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"commit_index": commit_index}, f)
        self._atomic_replace(tmp, self.commit_path)

    # ------------------------------------------------------------ snapshot
    def save_snapshot(self, last_included_index, last_included_term, state, last_req):
        """Атомарно + fsync. Только ПОСЛЕ успешного возврата можно обрезать лог (compact_log)."""
        tmp = self.snap_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"last_included_index": last_included_index,
                       "last_included_term": last_included_term,
                       "state": state, "last_req": last_req}, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        self._atomic_replace(tmp, self.snap_path)
        _fsync_dir(self.dir)
        self.stats["snapshots_written"] += 1
        self.stats["fsyncs"] += 1

    def compact_log(self, tail, last_included_index):
        """Оставить в log.jsonl только tail (записи с индексом > last_included_index)."""
        if tail and tail[0]["index"] != last_included_index + 1:
            raise ValueError("tail должен начинаться с last_included_index + 1")
        self.rewrite_log(tail, last_included_index)
        self.stats["compactions"] += 1

    def rewrite_log(self, entries, base):
        """Атомарная перезапись log.jsonl (tmp + fsync + replace)."""
        tmp = self.log_path + ".tmp"
        offsets, pos = [], 0
        with open(tmp, "wb") as f:
            for e in entries:
                line = _encode(e)
                offsets.append(pos)
                pos += len(line)
                f.write(line)
            f.flush()
            os.fsync(f.fileno())

        # Windows: os.replace не может заменить файл, пока он открыт хоть одним handle,
        # даже нашим собственным self._f. Закрываем перед заменой.
        if self._f is not None:
            try:
                self._f.close()
            except OSError:
                pass
            self._f = None

        self._atomic_replace(tmp, self.log_path)
        _fsync_dir(self.dir)
        self.offsets = offsets
        self.base = base
        self._open()
        self.stats["fsyncs"] += 1

    def save_log(self, log):
        """Полная перезапись (простой вариант из Lab 5); base берётся из первой записи."""
        self.rewrite_log(log, log[0]["index"] - 1 if log else self.base)

    # ------------------------------------------------------------ append / truncate
    def append_entries(self, entries):
        if not entries:
            return
        pos = self._f.seek(0, os.SEEK_END)
        buf = []
        for e in entries:
            if e["index"] != self.base + len(self.offsets) + 1:
                raise ValueError(f"нарушен порядок индексов: ждали "
                                 f"{self.base + len(self.offsets) + 1}, пришёл {e['index']}")
            self.offsets.append(pos)
            line = _encode(e)
            buf.append(line)
            pos += len(line)
        self._f.write(b"".join(buf))
        self._sync()
        self.stats["log_entries_written"] += len(entries)

    def truncate_from(self, index):
        """Удалить записи index, index+1, ... (конфликт лога на follower'е)."""
        pos = index - self.base - 1
        if pos < 0:
            raise ValueError("нельзя обрезать лог внутри snapshot'а")
        if pos >= len(self.offsets):
            return
        off = self.offsets[pos]
        del self.offsets[pos:]
        self._f.flush()
        self._f.truncate(off)
        self._f.seek(0, os.SEEK_END)
        self._sync()
        self.stats["truncations"] += 1

    def close(self):
        if self._f:
            self._f.close()
            self._f = None
