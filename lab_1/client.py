import json
import logging
import socket
import time

logging.basicConfig(level=logging.INFO, format='[CLIENT] %(asctime)s %(message)s')

HOST = '127.0.0.1'
PORT = 9000
TIMEOUT = 1.5  # секунды
MAX_RETRIES = 3
NUM_REQUESTS = 25


class LamportClient:
    def __init__(self):
        self.clock = 0

    def tick(self):
        self.clock += 1
        return self.clock

    def update(self, received_ts):
        self.clock = max(self.clock, received_ts) + 1
        return self.clock

    def send_request(self, message, request_id):
        for attempt in range(1, MAX_RETRIES + 1):
            ts = self.tick()
            payload = {
                'request_id': request_id,
                'message': message,
                'timestamp': ts,
                'attempt': attempt
            }
            start = time.perf_counter()
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(TIMEOUT)
                    s.connect((HOST, PORT))
                    s.sendall((json.dumps(payload) + '\n').encode('utf-8'))
                    data = s.recv(4096)
                    if not data:
                        raise ConnectionError('Empty response')
                    response = json.loads(data.decode('utf-8'))
                    latency = time.perf_counter() - start
                    server_ts = response.get('server_timestamp', 0)
                    self.update(server_ts)
                    logging.info(
                        f'Req #{request_id} OK | client_ts={ts} '
                        f'server_ts={server_ts} latency={latency * 1000:.1f}ms '
                        f'attempt={attempt}'
                    )
                    return {
                        'success': True,
                        'latency': latency,
                        'client_ts': ts,
                        'server_ts': server_ts,
                        'attempts': attempt
                    }
            except (socket.timeout, ConnectionRefusedError, ConnectionError, OSError) as e:
                logging.warning(
                    f'Req #{request_id} attempt {attempt} failed: {type(e).__name__}'
                )
                if attempt == MAX_RETRIES:
                    logging.error(f'Req #{request_id} FAILED after {MAX_RETRIES} attempts')
                    return {'success': False, 'attempts': attempt}
                time.sleep(0.2 * attempt)  # простой backoff
        return {'success': False}

    def run(self):
        results = []
        for i in range(1, NUM_REQUESTS + 1):
            res = self.send_request(f'hello-{i}', i)

            results.append(res)
            time.sleep(0.05)  # небольшая пауза между запросами
            # Статистика
        ok = [r for r in results if r.get('success')]
        print('\n=== SUMMARY ===')
        print(f'Total: {len(results)} | Success: {len(ok)} | Failed: {len(results) - len(ok)}')
        if ok:
            latencies = [r['latency'] * 1000 for r in ok]
            print(
                f'Latency ms: min={min(latencies):.1f} avg={sum(latencies) / len(latencies):.1f} max={max(latencies):.1f}')
        print(f'Final client clock: {self.clock}')


if __name__ == '__main__':
    client = LamportClient()
    client.run()
