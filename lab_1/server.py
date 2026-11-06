import json
import logging
import random
import socket
import threading
import time

logging.basicConfig(level=logging.INFO, format='[SERVER] %(asctime)s %(message)s')

HOST = '127.0.0.1'
PORT = 9000


class LamportServer:
    def __init__(self):
        self.clock = 0
        self.lock = threading.Lock()
        # Режим симуляции отказов (меняется в экспериментах)
        self.delay_chance = 0.3  # вероятность долгой обработки
        self.delay_range = (1.0, 2.5)  # секунды
        self.crash_chance = 0.4  # вероятность «игнорировать» запрос

    def update_clock(self, received_ts=None):
        with self.lock:
            if received_ts is not None:
                self.clock = max(self.clock, received_ts) + 1
            else:
                self.clock += 1
            return self.clock

    def handle_client(self, conn, addr):
        logging.info(f'Connected by {addr}')
        try:
            data = conn.recv(4096)
            if not data:
                return
            request = json.loads(data.decode('utf-8'))
            client_ts = request.get('timestamp', 0)
            msg = request.get('message', '')

            # Симуляция частичного отказа / долгой обработки
            if random.random() < self.crash_chance:
                logging.warning('Simulating crash — dropping request')
                return  # просто не отвечаем

            if random.random() < self.delay_chance:
                delay = random.uniform(*self.delay_range)
                logging.info(f'Simulating delay {delay:.2f}s')
                time.sleep(delay)

            server_ts = self.update_clock(client_ts)
            response = {
                'status': 'ok',
                'echo': msg,
                'server_timestamp': server_ts,
                'client_timestamp_received': client_ts
            }
            conn.sendall((json.dumps(response) + '\n').encode('utf-8'))
            logging.info(f'Responded ts={server_ts} to client_ts={client_ts}')
        except Exception as e:
            logging.error(f'Error: {e}')
        finally:
            conn.close()

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((HOST, PORT))
            s.listen(5)
            logging.info(f'Server listening on {HOST}:{PORT}')
            while True:
                conn, addr = s.accept()
                t = threading.Thread(target=self.handle_client, args=(conn, addr))
                t.daemon = True
                t.start()


if __name__ == '__main__':
    server = LamportServer()
    # Для начала оставьте вероятности отказов = 0
    server.start()
