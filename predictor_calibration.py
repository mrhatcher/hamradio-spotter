"""Predictor calibration report.

Reads the rolling predictions.db produced by app.py and reports how often
each confidence bucket actually converted into a QSO. Optional filters by
band, mode, time window, or path (MUT / RX / TX / ·). With --by-path,
prints a confidence × path cross-tab so you can see e.g. whether HIGH
predictions on TX-only paths actually convert.

Examples:
  python predictor_calibration.py
  python predictor_calibration.py --since 7d --band 20m
  python predictor_calibration.py --mode FT8 --details
  python predictor_calibration.py --by-path
  python predictor_calibration.py --path TX --since 24h
"""

from __future__ import annotations

import argparse
import sys
import time

import prediction_log


# Order to print buckets (descending confidence) regardless of dict iteration order.
_CONF_ORDER = ['HIGH', 'GOOD', 'MODERATE', 'LOW', 'UNLIKELY']

# Path column order for cross-tab display. '?' = pre-migration row
# with no recorded path; will phase out after the 7d prune window.
_PATH_ORDER = ['MUT', 'RX', 'TX', '·', '?']


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


def _print_flat(stats: dict) -> None:
    """Per-confidence one-column conversion table."""
    print(f"  {'Confidence':<10}  {'Total':>6}  {'Hits':>6}  {'Conv%':>7}")
    grand_total = 0
    grand_hits = 0
    ordered = [k for k in _CONF_ORDER if k in stats] + \
              [k for k in stats if k not in _CONF_ORDER]
    for bucket in ordered:
        b = stats[bucket]
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


def _print_by_path(grouped: dict) -> None:
    """Cross-tab: rows = confidence buckets, columns = paths.

    Each cell shows `hits/total  pct%`. Empty cells render as `.`.
    """
    # Collect every path actually present, then order by _PATH_ORDER + tail.
    all_paths: set[str] = set()
    for conf_map in grouped.values():
        all_paths.update(conf_map.keys())
    paths = [p for p in _PATH_ORDER if p in all_paths] + \
            [p for p in sorted(all_paths) if p not in _PATH_ORDER]

    if not paths:
        print("  No predictions in the window. Run the app for a session, "
              "then re-check.")
        return

    col_w = 14   # width per path cell ("12/45 26.7%")
    header = f"  {'Conf':<10}" + ''.join(f"{p:>{col_w}}" for p in paths) + f"{'TOTAL':>{col_w}}"
    print(header)
    print("-" * len(header))

    grand_total = 0
    grand_hits = 0
    ordered = [k for k in _CONF_ORDER if k in grouped] + \
              [k for k in grouped if k not in _CONF_ORDER]
    for bucket in ordered:
        conf_map = grouped[bucket]
        row_total = 0
        row_hits = 0
        cells = []
        for p in paths:
            cell = conf_map.get(p)
            if cell is None:
                cells.append(f"{'.':>{col_w}}")
            else:
                row_total += cell['total']
                row_hits += cell['hits']
                cells.append(
                    f"{cell['hits']:>3}/{cell['total']:<3} {cell['conversion_pct']:>5.1f}%".rjust(col_w)
                )
        total_pct = (row_hits / row_total * 100.0) if row_total else 0.0
        cells.append(f"{row_hits:>3}/{row_total:<3} {total_pct:>5.1f}%".rjust(col_w))
        grand_total += row_total
        grand_hits += row_hits
        print(f"  {bucket:<10}" + ''.join(cells))

    # Per-path column totals
    print("-" * len(header))
    col_totals = []
    for p in paths:
        c_total = c_hits = 0
        for conf_map in grouped.values():
            cell = conf_map.get(p)
            if cell:
                c_total += cell['total']
                c_hits += cell['hits']
        pct = (c_hits / c_total * 100.0) if c_total else 0.0
        col_totals.append(f"{c_hits:>3}/{c_total:<3} {pct:>5.1f}%".rjust(col_w))
    g_pct = (grand_hits / grand_total * 100.0) if grand_total else 0.0
    col_totals.append(f"{grand_hits:>3}/{grand_total:<3} {g_pct:>5.1f}%".rjust(col_w))
    print(f"  {'TOTAL':<10}" + ''.join(col_totals))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--since', default='7d',
                    help='Time window: 7d / 24h / 90m / seconds. Default: 7d.')
    ap.add_argument('--band', help='Restrict to this band (e.g. 20m).')
    ap.add_argument('--mode', help='Restrict to this mode (e.g. FT8).')
    ap.add_argument('--path', choices=['MUT', 'RX', 'TX', '·'],
                    help='Restrict to a single path symbol.')
    ap.add_argument('--by-path', action='store_true',
                    help='Cross-tab confidence × path. Ignores --path.')
    ap.add_argument('--details', action='store_true',
                    help='Also dump the last 30 predictions for inspection.')
    args = ap.parse_args(argv)

    since_ts = _parse_since(args.since)
    plog = prediction_log.get_log()

    filters = [f"since={args.since}"] if args.since else []
    if args.band: filters.append(f"band={args.band}")
    if args.mode: filters.append(f"mode={args.mode}")
    if args.path and not args.by_path: filters.append(f"path={args.path}")

    if args.by_path:
        print(f"Calibration by confidence × path  [{'  '.join(filters) or 'all-time'}]")
        print()
        grouped = plog.calibration_stats(
            since_ts=since_ts, band=args.band, mode=args.mode, group_by_path=True,
        )
        _print_by_path(grouped)
    else:
        print(f"Calibration report  [{'  '.join(filters) or 'all-time'}]")
        print("-" * 60)
        stats = plog.calibration_stats(
            since_ts=since_ts, band=args.band, mode=args.mode, path=args.path,
        )
        _print_flat(stats)

    if args.details:
        print()
        print("Latest 30 predictions:")
        for r in plog.recent_predictions(limit=30):
            ts = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(r['ts']))
            hit = 'HIT ' if r['matched_qso_ts'] else '  - '
            p = r.get('path') or '?'
            print(f"  {ts}  {hit}  {r['callsign']:<8}  "
                  f"{r.get('band') or '?':<5}  {r.get('mode') or '?':<5}  "
                  f"{p:<3}  score={r['score']:>3}  {r['confidence']:<9}  "
                  f"rank={r.get('rank') or '?'}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
