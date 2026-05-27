"""Rolling log of Contact Probability predictions + QSO-outcome matching.

Every panel refresh, the displayed top-7 are snapshotted into the
`predictions` table. When a new QSO appears in the log feed, the most
recent prediction for that callsign within the lookback window is
marked `matched_qso_ts`. The result is a calibration record: for each
confidence bucket (HIGH/GOOD/MODERATE/LOW/UNLIKELY), what fraction of
predictions converted into a real QSO?

Storage: SQLite at <project_dir>/predictions.db. Rows older than
PRUNE_DAYS are dropped at startup.
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
from typing import Optional


DEFAULT_DB_PATH = os.path.join(os.path.dirname(__file__), 'predictions.db')
DEFAULT_LOOKBACK_SECS = 300        # 5 minutes — the QSO-match window
PRUNE_DAYS = 7                     # ~200k rows tops at 3s refresh × 7 panel rows


_SCHEMA = """
CREATE TABLE IF NOT EXISTS predictions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ts              REAL    NOT NULL,
    callsign        TEXT    NOT NULL,
    band            TEXT,
    mode            TEXT,
    score           INTEGER NOT NULL,
    confidence      TEXT    NOT NULL,
    snr_fwd         INTEGER,
    snr_rev         INTEGER,
    state           TEXT,
    recommendation  TEXT,
    rank            INTEGER,
    matched_qso_ts  REAL
);
CREATE INDEX IF NOT EXISTS idx_predictions_cs_ts ON predictions(callsign, ts);
CREATE INDEX IF NOT EXISTS idx_predictions_ts    ON predictions(ts);
"""


class PredictionLog:
    """Thread-safe SQLite-backed prediction store.

    Single connection w/ a lock — calls are infrequent (every GUI refresh
    or on QSO arrival) and cheap. WAL mode for crash safety.
    """

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.path = db_path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        with self._lock:
            self._conn.executescript(_SCHEMA)
        self.prune_old()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def snapshot(self, rows: list[dict]) -> None:
        """Bulk-insert one panel-refresh worth of predictions."""
        if not rows:
            return
        now = time.time()
        params = [
            (
                row.get('ts', now),
                row['callsign'],
                row.get('band'),
                row.get('mode'),
                int(row['score']),
                row['confidence'],
                row.get('snr_fwd'),
                row.get('snr_rev'),
                row.get('state'),
                row.get('recommendation'),
                row.get('rank'),
            )
            for row in rows
        ]
        with self._lock:
            self._conn.executemany(
                """
                INSERT INTO predictions
                  (ts, callsign, band, mode, score, confidence,
                   snr_fwd, snr_rev, state, recommendation, rank)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                params,
            )

    def match_qso(
        self,
        callsign: str,
        qso_ts: Optional[float] = None,
        band: Optional[str] = None,
        mode: Optional[str] = None,
        lookback_secs: int = DEFAULT_LOOKBACK_SECS,
    ) -> Optional[dict]:
        """Mark the most recent prediction for callsign as matched by a QSO.

        Looks at predictions within `lookback_secs` of qso_ts, picks the
        latest one that isn't already matched, sets matched_qso_ts.
        If band/mode are given, restricts to predictions on the same
        band+mode (avoids mis-attributing across band-hops).
        Returns the matched row's data, or None if no eligible prediction.
        """
        if qso_ts is None:
            qso_ts = time.time()
        lo = qso_ts - lookback_secs
        where = [
            "callsign = ?",
            "ts BETWEEN ? AND ?",
            "matched_qso_ts IS NULL",
        ]
        params: list = [callsign.upper(), lo, qso_ts]
        if band:
            where.append("band = ?")
            params.append(band)
        if mode:
            where.append("mode = ?")
            params.append(mode)
        sql = (
            "SELECT id, ts, score, confidence, band, mode, rank "
            "FROM predictions WHERE " + " AND ".join(where) +
            " ORDER BY ts DESC LIMIT 1"
        )
        with self._lock:
            cur = self._conn.execute(sql, params)
            row = cur.fetchone()
            if not row:
                return None
            pred_id = row[0]
            self._conn.execute(
                "UPDATE predictions SET matched_qso_ts = ? WHERE id = ?",
                (qso_ts, pred_id),
            )
        return {
            'id': pred_id, 'ts': row[1], 'score': row[2],
            'confidence': row[3], 'band': row[4], 'mode': row[5],
            'rank': row[6], 'matched_qso_ts': qso_ts,
        }

    def calibration_stats(
        self,
        since_ts: Optional[float] = None,
        band: Optional[str] = None,
        mode: Optional[str] = None,
    ) -> dict:
        """Conversion rate per confidence bucket.

        For each unique callsign+band+mode within the window we count
        ONE row (the latest), so we're not double-counting a station
        that sat in the panel for many refreshes.
        """
        where_parts = []
        params: list = []
        if since_ts is not None:
            where_parts.append("ts >= ?")
            params.append(since_ts)
        if band:
            where_parts.append("band = ?")
            params.append(band)
        if mode:
            where_parts.append("mode = ?")
            params.append(mode)
        where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

        sql = f"""
        WITH latest AS (
            SELECT callsign, band, mode,
                   MAX(ts) AS latest_ts
            FROM predictions
            {where_sql}
            GROUP BY callsign, band, mode
        )
        SELECT p.confidence,
               COUNT(*)                                            AS total,
               SUM(CASE WHEN p.matched_qso_ts IS NOT NULL THEN 1 ELSE 0 END) AS hits
        FROM predictions p
        JOIN latest l
          ON p.callsign = l.callsign
         AND COALESCE(p.band,'')  = COALESCE(l.band,'')
         AND COALESCE(p.mode,'')  = COALESCE(l.mode,'')
         AND p.ts = l.latest_ts
        GROUP BY p.confidence
        """
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        out: dict[str, dict] = {}
        for conf, total, hits in rows:
            pct = (hits / total * 100.0) if total else 0.0
            out[conf] = {'total': total, 'hits': hits, 'conversion_pct': pct}
        return out

    def recent_predictions(self, limit: int = 50) -> list[dict]:
        """Latest N predictions, newest first — for debugging / inspection."""
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT ts, callsign, band, mode, score, confidence,
                       snr_fwd, snr_rev, state, recommendation, rank, matched_qso_ts
                FROM predictions
                ORDER BY ts DESC
                LIMIT ?
                """,
                (limit,),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]

    def prune_old(self, max_age_days: float = PRUNE_DAYS) -> int:
        """Drop rows older than max_age_days. Returns count deleted."""
        cutoff = time.time() - (max_age_days * 86400.0)
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM predictions WHERE ts < ?",
                (cutoff,),
            )
            return cur.rowcount


# Module-level singleton for app.py to import
_singleton: Optional[PredictionLog] = None
_singleton_lock = threading.Lock()


def get_log(db_path: str = DEFAULT_DB_PATH) -> PredictionLog:
    """Lazy singleton — open the SQLite once, share across threads."""
    global _singleton
    with _singleton_lock:
        if _singleton is None:
            _singleton = PredictionLog(db_path)
    return _singleton
