import concurrent.futures
import json
import logging
import socket
import sys
import asyncio
logging.basicConfig(level=logging.INFO, format='[CLIENT] %(message)s')

REPLICAS = [
    ('127.0.0.1', 9001),
    ('127.0.0.1', 9002),
    ('127.0.0.1', 9003),
]
N, W, R = 3, 2, 2
TIMEOUT = 1.0


def send_to_replica(host, port, request):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(TIMEOUT)
            s.connect((host, port))
            s.sendall((json.dumps(request) + '\n').encode())
            data = s.recv(4096)
            return json.loads(data.decode())
    except Exception as e:
        return {'status': 'error', 'error': str(e)}


def compare_vectors(v1, v2):
    less = greater = False
    for a, b in zip(v1, v2):
        if a < b: less = True
        if a > b: greater = True
    if less and greater: return 'concurrent'
    if less: return 'less'
    if greater: return 'greater'
    return 'equal'


def quorum_put(key, value):
    req = {'op': 'PUT', 'key': key, 'value': value}
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=N) as ex:
        futs = [ex.submit(send_to_replica, h, p, req) for h, p in REPLICAS]
        for f in concurrent.futures.as_completed(futs):
            results.append(f.result())
    ok = [r for r in results if r.get('status') == 'ok']
    logging.info(f'PUT {key}={value} → {len(ok)}/{N} successes')
    if len(ok) >= W:
        return {'success': True, 'acks': len(ok), 'details': ok}
    return {'success': False, 'acks': len(ok), 'details': results}


def quorum_get(key):
    req = {'op': 'GET', 'key': key}
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=N) as ex:
        futs = [ex.submit(send_to_replica, h, p, req) for h, p in REPLICAS]
        for f in concurrent.futures.as_completed(futs):
            results.append(f.result())
    ok = [r for r in results if r.get('status') == 'ok']
    if len(ok) < R:
        return {'success': False, 'reason': 'quorum not reached', 'details': results}

    # Выбор самого «нового» значения по векторным часам
    best = ok[0]
    conflict = False
    for r in ok[1:]:
        cmp = compare_vectors(r['vc'], best['vc'])
        if cmp == 'greater':
            best = r
            conflict = False
        elif cmp == 'concurrent':
            conflict = True
    return {
        'success': True,
        'value': best['value'],
        'vc': best['vc'],
        'conflict': conflict,
        'replicas_responded': len(ok),
        'all_versions': [(r['replica_id'], r['value'], r['vc']) for r in ok]
    }


if __name__ == "__main__":
    scenario = int(sys.argv[1])
    match scenario:
        case 1:
            print(quorum_put('name', 'gleb'))
            print(quorum_put('age', '21'))
            print(quorum_get('name'))
            print(quorum_get('age'))
        case 2:
            print(quorum_put('name', 'gleb'))
            print(quorum_put('age', '21'))
            print(quorum_get('name'))
            print(quorum_get('work'))
        case 3:
            input("stop replica with id 0")
            print(quorum_put('name', 'gleb'))
            print(quorum_get('name'))
            input("stop replica with id 1")
            print(quorum_put('age', '21'))
            print(quorum_get('age'))
        case 4:
            results = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=N) as ex:
                futs = [
                    ex.submit(send_to_replica, '127.0.0.1', 9001, {'op': 'PUT', 'key': "name", 'value': "gleb"}),
                    ex.submit(send_to_replica, '127.0.0.1', 9001, {'op': 'PUT', 'key': "age", 'value': "21"}),
                    ex.submit(send_to_replica, '127.0.0.1', 9002, {'op': 'PUT', 'key': "age", 'value': "21"})]
                for f in concurrent.futures.as_completed(futs):
                    results.append(f.result())
            print(results)
            print(quorum_get("age"))

