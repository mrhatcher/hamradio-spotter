"""Predictor calibration report.

Reads the rolling predictions.db produced by app.py and reports how often
each confidence bucket actually converted into a QSO. Optional filters by
band, mode, or time window.

Examples:
  python predictor_calibration.py
  python predictor_calibration.py --since 7d --band 20m
  python predictor_calibration.py --mode FT8 --details
"""

from __future__ import annotations

import argparse
import sys
import time

import prediction_log


# Order to print buckets (descending confidence) regardless of dict iteration order.
_CONF_ORDER = ['HIGH', 'GOOD', 'MODERATE', 'LOW', 'UNLIKELY']


def _parse_since(s: str) -> float:
    """Parse '7d', '24h', '90m' (or any unsuffixed seconds) into unix ts."""
    if not s:
        return 0.0
    s = s.strip().lower()
    if s.endswith('d'):
        secs = float(s[:-1]) * 86400.0
    elif s.endswith('h'):
        secs = float(s[:-1]) * 3600.0
    elif s.endswith('m'):
        secs = float(s[:-1]) * 60.0
    else:
        secs = float(s)
    return time.time() - secs


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--since', default='7d',
                    help='Time window: 7d / 24h / 90m / seconds. Default: 7d.')
    ap.add_argument('--band', help='Restrict to this band (e.g. 20m).')
    ap.add_argument('--mode', help='Restrict to this mode (e.g. FT8).')
    ap.add_argument('--details', action='store_true',
                    help='Also dump the last 30 predictions for inspection.')
    args = ap.parse_args(argv)

    since_ts = _parse_since(args.since)
    plog = prediction_log.get_log()
    stats = plog.calibration_stats(
        since_ts=since_ts,
        band=args.band,
        mode=args.mode,
    )

    filters = []
    if args.since:
        filters.append(f"since={args.since}")
    if args.band:
        filters.append(f"band={args.band}")
    if args.mode:
        filters.append(f"mode={args.mode}")
    print(f"Calibration report  [{'  '.join(filters) or 'all-time'}]")
    print("-" * 60)
    print(f"  {'Confidence':<10}  {'Total':>6}  {'Hits':>6}  {'Conv%':>7}")
    grand_total = 0
    grand_hits = 0
    for bucket in _CONF_ORDER:
        b = stats.get(bucket)
        if not b:
            continue
        grand_total += b['total']
        grand_hits += b['hits']
        print(f"  {bucket:<10}  {b['total']:>6}  {b['hits']:>6}  {b['conversion_pct']:>6.1f}%")
    # Surface buckets we don't pre-list (defensive — confidence labels may
    # shift in the predictor over time).
    for bucket, b in stats.items():
        if bucket in _CONF_ORDER:
            continue
        grand_total += b['total']
        grand_hits += b['hits']
        print(f"  {bucket:<10}  {b['total']:>6}  {b['hits']:>6}  {b['conversion_pct']:>6.1f}%")

    print("-" * 60)
    if grand_total:
        pct = grand_hits / grand_total * 100.0
        print(f"  {'TOTAL':<10}  {grand_total:>6}  {grand_hits:>6}  {pct:>6.1f}%")
    else:
        print("  No predictions in the window. Run the app for a session, "
              "then re-check.")

    if args.details:
        print()
        print("Latest 30 predictions:")
        for r in plog.recent_predictions(limit=30):
            ts = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(r['ts']))
            hit = 'HIT ' if r['matched_qso_ts'] else '  - '
            print(f"  {ts}  {hit}  {r['callsign']:<8}  "
                  f"{r.get('band') or '?':<5}  {r.get('mode') or '?':<5}  "
                  f"score={r['score']:>3}  {r['confidence']:<9}  "
                  f"rank={r.get('rank') or '?'}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
