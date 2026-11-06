"""
    meta.json    {"current_term": int, "voted_for": int|null}
                 атомарная замена: tmp -> flush -> fsync -> os.replace -> fsync(каталога)
    log.jsonl    append-only лог: одна запись на строку
                     <crc32 hex, 8 симв.>\t<json записи>\n
                 запись добавляется и сразу fsync'ается; при обрезке хвоста файл усекается
                 до байтового смещения нужной записи (ftruncate) + fsync
    commit.json  {"commit_index": int} - подсказка для быстрого восстановления state machine.
                 Безопасна, т.к. пишется ПОСЛЕ того, как записи уже fsync'нуты в log.jsonl, а
                 закоммиченные записи в Raft не откатываются. Устаревшее (меньшее) значение
                 тоже безопасно - узел просто догонит через AppendEntries. Без fsync (os.replace
                 всё равно атомарен), при чтении значение ограничивается длиной лога.

Защита от битых данных:
  * строка без '\n' на конце (оборванная запись при kill -9 / потере питания) отбрасывается;
  * строка с неверным CRC / не-JSON / неверным индексом => лог усекается ДО неё
    (всё, что после повреждения, недостоверно);
  * повреждённый/пустой meta.json => term=0, voted_for=None.
"""
import json
import os
import time
import zlib


def _fsync_dir(path):
    """fsync каталога, чтобы rename/создание файла пережили сбой питания (best effort)."""
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
        self._f = None
        self.offsets = []                  # offsets[i] - байтовое смещение начала записи index=i+1
        self.stats = {"meta_writes": 0, "log_entries_written": 0, "fsyncs": 0,
                      "truncations": 0, "recovery_ms": 0.0, "dropped_bytes": 0,
                      "meta_corrupt": False}

    # ------------------------------------------------------------ чтение
    def load(self):
        """-> {'current_term', 'voted_for', 'commit_index', 'log'}; при любых проблемах - старт с нуля."""
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

        log = self._load_log()

        if os.path.exists(self.commit_path):
            try:
                with open(self.commit_path, encoding="utf-8") as f:
                    commit = int(json.load(f)["commit_index"])
            except (OSError, ValueError, KeyError, TypeError):
                commit = 0
        commit = max(0, min(commit, len(log)))

        self.stats["recovery_ms"] = round((time.time() - t0) * 1000, 2)
        return {"current_term": term, "voted_for": voted_for, "commit_index": commit, "log": log}

    def _load_log(self):
        log, self.offsets = [], []
        good_end = 0
        if os.path.exists(self.log_path):
            with open(self.log_path, "rb") as f:
                raw = f.read()
            pos = 0
            while pos < len(raw):
                nl = raw.find(b"\n", pos)
                if nl == -1:
                    break                                   # оборванная последняя запись
                line = raw[pos:nl]
                entry = self._decode(line, expected_index=len(log) + 1)
                if entry is None:
                    break                                   # повреждение: всё дальше недостоверно
                self.offsets.append(pos)
                log.append(entry)
                pos = nl + 1
                good_end = pos
            if good_end < len(raw):
                self.stats["dropped_bytes"] = len(raw) - good_end
                with open(self.log_path, "r+b") as f:       # выбрасываем мусор в хвосте
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
        try:
            crc_hex, body = line.split(b"\t", 1)
            if int(crc_hex, 16) != (zlib.crc32(body) & 0xFFFFFFFF):
                return None
            e = json.loads(body.decode("utf-8"))
            if e["index"] != expected_index or "term" not in e or "command" not in e:
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

    # ------------------------------------------------------------ запись
    def save_hard_state(self, current_term, voted_for):
        """Атомарно + fsync. Вызывать ДО ответа на RequestVote / продолжения протокола."""
        tmp = self.meta_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"current_term": current_term, "voted_for": voted_for}, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.meta_path)
        _fsync_dir(self.dir)
        self.stats["meta_writes"] += 1
        self.stats["fsyncs"] += 1

    def save_commit(self, commit_index):
        tmp = self.commit_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"commit_index": commit_index}, f)
        os.replace(tmp, self.commit_path)

    def append_entries(self, entries):
        """Дописать в конец лога и fsync. Записи должны идти подряд (index = len+1, ...)."""
        if not entries:
            return
        pos = self._f.seek(0, os.SEEK_END)
        buf = []
        for e in entries:
            if e["index"] != len(self.offsets) + 1:
                raise ValueError(f"нарушен порядок индексов: ждали {len(self.offsets) + 1}, "
                                 f"пришёл {e['index']}")
            self.offsets.append(pos)
            line = _encode(e)
            buf.append(line)
            pos += len(line)
        self._f.write(b"".join(buf))
        self._sync()
        self.stats["log_entries_written"] += len(entries)

    def truncate_from(self, index):
        """Удалить записи index, index+1, ... (конфликт лога на follower'е)."""
        if index > len(self.offsets):
            return
        off = self.offsets[index - 1]
        del self.offsets[index - 1:]
        self._f.flush()
        self._f.truncate(off)
        self._f.seek(0, os.SEEK_END)
        self._sync()
        self.stats["truncations"] += 1

    def save_log(self, log):
        """Полная атомарная перезапись лога (простой вариант из задания; в узле не используется)."""
        tmp = self.log_path + ".tmp"
        offsets, pos = [], 0
        with open(tmp, "wb") as f:
            for e in log:
                line = _encode(e)
                offsets.append(pos)
                pos += len(line)
                f.write(line)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.log_path)
        _fsync_dir(self.dir)
        self.offsets = offsets
        self._open()
        self.stats["fsyncs"] += 1

    def close(self):
        if self._f:
            self._f.close()
            self._f = None