import json
import logging
import socket
import sys
import threading

logging.basicConfig(level=logging.INFO,
                    format='[REPLICA-%(replica_id)s] %(asctime)s %(message)s')


class VectorClock:
    def __init__(self, size, replica_id):
        self.size = size
        self.replica_id = replica_id
        self.clock = [0] * size

    def tick(self):
        self.clock[self.replica_id] += 1
        return list(self.clock)

    def update(self, other):
        for i in range(self.size):
            self.clock[i] = max(self.clock[i], other[i])
        self.clock[self.replica_id] += 1
        return list(self.clock)

    def merge(self, other):  # без увеличения своей компоненты
        for i in range(self.size):
            self.clock[i] = max(self.clock[i], other[i])


def compare_vectors(v1, v2):
    """Возвращает: 'less', 'greater', 'equal', 'concurrent'"""
    less = greater = False
    for a, b in zip(v1, v2):
        if a < b: less = True
        if a > b: greater = True
    if less and greater: return 'concurrent'
    if less: return 'less'
    if greater: return 'greater'
    return 'equal'


class Replica:
    def __init__(self, replica_id, port, n=3):
        self.replica_id = replica_id
        self.port = port
        self.n = n
        self.vc = VectorClock(n, replica_id)
        self.data = {}  # key -> {'value': ..., 'vc': [...]}
        self.lock = threading.Lock()

    def handle_put(self, key, value, client_vc=None):
        with self.lock:
            if client_vc:
                self.vc.merge(client_vc)
            new_vc = self.vc.tick()
            self.data[key] = {'value': value, 'vc': new_vc}
            logging.info(f'PUT {key}={value} vc={new_vc}',
                         extra={'replica_id': self.replica_id})
            return {'status': 'ok', 'vc': new_vc, 'replica_id': self.replica_id}

    def handle_get(self, key):
        with self.lock:
            if key not in self.data:
                return {'status': 'not_found', 'replica_id': self.replica_id}
            entry = self.data[key]
            return {
                'status': 'ok',
                'value': entry['value'],
                'vc': entry['vc'],
                'replica_id': self.replica_id
            }

    def handle_client(self, conn, addr):
        try:
            data = conn.recv(4096)
            if not data: return
            req = json.loads(data.decode())
            op = req.get('op')
            if op == 'PUT':
                resp = self.handle_put(req['key'], req['value'], req.get('vc'))
            elif op == 'GET':
                resp = self.handle_get(req['key'])
            else:
                resp = {'status': 'error', 'message': 'unknown op'}
            conn.sendall((json.dumps(resp) + '\n').encode())
        except Exception as e:
            logging.error(f'Error: {e}', extra={'replica_id': self.replica_id})
        finally:
            conn.close()

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('127.0.0.1', self.port))
            s.listen(5)
            logging.info(f'Listening on port {self.port}',
                         extra={'replica_id': self.replica_id})
            while True:
                conn, addr = s.accept()
                t = threading.Thread(target=self.handle_client, args=(conn, addr))
                t.daemon = True
                t.start()


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: python replica.py <replica_id 0|1|2> <port>')
        sys.exit(1)
    rid = int(sys.argv[1])
    port = int(sys.argv[2])
    Replica(rid, port).start()
