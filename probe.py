"""Quick UDP probe — logs first packet received on each port to probe_out.txt"""
import socket, threading, time, os

PORTS = [2234, 2237, 2333, 2335, 2336, 2337, 2338]
OUT   = os.path.join(os.path.dirname(__file__), 'probe_out.txt')

results = []

def listen(port):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('0.0.0.0', port))
        s.settimeout(15)
        data, addr = s.recvfrom(65535)
        results.append(f"PORT {port}: got {len(data)} bytes from {addr}  first4={data[:4].hex()}")
    except socket.timeout:
        results.append(f"PORT {port}: no packets in 15s")
    except Exception as e:
        results.append(f"PORT {port}: error — {e}")

threads = [threading.Thread(target=listen, args=(p,), daemon=True) for p in PORTS]
for t in threads: t.start()
for t in threads: t.join(timeout=16)

with open(OUT, 'w') as f:
    f.write('\n'.join(results) + '\n')
print("done")
