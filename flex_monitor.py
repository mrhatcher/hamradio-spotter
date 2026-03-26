"""FlexRadio 6600 TCP API monitor.

Connects to the SmartSDR TCP command API (default port 4992),
subscribes to slice status updates, and fires a callback whenever
a slice's mode, frequency, or in_use state changes.

Protocol reference:
  https://github.com/flexradio/smartsdr-api-docs/wiki/SmartSDR-TCPIP-API
"""
from __future__ import annotations

import re
import socket
import time
from typing import Callable, Optional


# Regex to parse slice status lines:
#   S<handle>|slice <n> <key=value> ...
_SLICE_RE = re.compile(
    r'^S[0-9A-Fa-f]+\|slice\s+(\d+)\s+(.+)$'
)


class FlexMonitor:
    """Persistent TCP connection to a FlexRadio SmartSDR command API."""

    def __init__(self, ip: str, port: int = 4992) -> None:
        self.ip = ip
        self.port = port
        self._sock: Optional[socket.socket] = None
        self._buf = b''
        self._seq = 0
        self.version = ''
        self.handle = ''

    # -- connection -----------------------------------------------------------

    def connect(self) -> None:
        """Open TCP socket and read the version + handle handshake lines."""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.settimeout(10.0)
        self._sock.connect((self.ip, self.port))
        self._sock.settimeout(5.0)
        self._buf = b''

        # Radio sends two lines: V<version> and H<handle>
        for _ in range(2):
            line = self._readline()
            if line.startswith('V'):
                self.version = line[1:]
            elif line.startswith('H'):
                self.handle = line[1:]
        print(f"[FLEX] Connected to {self.ip}:{self.port}  "
              f"version={self.version}  handle={self.handle}")

    def disconnect(self) -> None:
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None

    # -- commands -------------------------------------------------------------

    def _send(self, cmd: str) -> int:
        """Send a command and return the sequence number used."""
        self._seq += 1
        msg = f"C{self._seq}|{cmd}\n"
        self._sock.sendall(msg.encode('utf-8'))
        return self._seq

    def subscribe_slices(self) -> None:
        """Subscribe to all slice status updates."""
        self._send("sub slice all")
        print("[FLEX] Subscribed to slice status updates")

    # -- read loop ------------------------------------------------------------

    def read_loop(self, callback: Callable[[int, str, bool, float], None]) -> None:
        """Block forever reading status lines. Calls callback on slice updates.

        callback(slice_num, mode, in_use, rf_frequency_mhz)

        Raises on connection loss so caller can reconnect.
        """
        self._sock.settimeout(30.0)  # longer timeout for idle monitoring

        while True:
            try:
                line = self._readline()
            except socket.timeout:
                # Send a keepalive ping
                try:
                    self._send("ping")
                except Exception:
                    raise ConnectionError("Keepalive failed")
                continue

            if not line:
                raise ConnectionError("Connection closed by radio")

            parsed = self.parse_slice_status(line)
            if parsed:
                callback(
                    parsed['slice_num'],
                    parsed.get('mode', ''),
                    parsed.get('in_use', False),
                    parsed.get('rf_frequency', 0.0),
                )

    # -- parsing --------------------------------------------------------------

    @staticmethod
    def parse_slice_status(line: str) -> Optional[dict]:
        """Parse a slice status line into a dict, or None if not a slice status.

        Input format: S<handle>|slice <n> key1=val1 key2=val2 ...
        Returns: {'slice_num': int, 'mode': str, 'in_use': bool, 'rf_frequency': float, ...}
        """
        m = _SLICE_RE.match(line)
        if not m:
            return None

        slice_num = int(m.group(1))
        pairs_str = m.group(2)

        result: dict = {'slice_num': slice_num}
        for pair in pairs_str.split():
            if '=' not in pair:
                continue
            key, val = pair.split('=', 1)
            if key == 'mode':
                result['mode'] = val.upper()
            elif key == 'in_use':
                result['in_use'] = val == '1'
            elif key == 'RF_frequency':
                try:
                    result['rf_frequency'] = float(val)
                except ValueError:
                    pass
            elif key == 'active':
                result['active'] = val == '1'

        return result

    # -- internal -------------------------------------------------------------

    def _readline(self) -> str:
        """Read one newline-terminated line from the socket."""
        while b'\n' not in self._buf:
            chunk = self._sock.recv(4096)
            if not chunk:
                raise ConnectionError("Connection closed")
            self._buf += chunk

        line_bytes, self._buf = self._buf.split(b'\n', 1)
        return line_bytes.decode('utf-8', errors='replace').strip()
