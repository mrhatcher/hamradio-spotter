"""
eQSL Inbox Verifier
===================
Cross-check eQSL.cc inbox entries (uploaded by other stations claiming a QSO
with you) against your JTDX ADIF log. Confirms whether each claimed contact
actually happened by matching callsign + date + time within a tolerance window.

Usage:
    python eqsl_verify.py <adif_path> --csv <eqsl_csv> [--window 10]
    python eqsl_verify.py <adif_path> --stdin            [--window 10]

Input CSV / stdin format (header optional):
    callsign,date,time,mode
    W1AW,2026-05-24,14:32,FT8
    K1ABC,20260523,1815,SSB

Date accepts YYYY-MM-DD or YYYYMMDD. Time accepts HH:MM or HHMM (UTC).
Mode is optional; blank acts as wildcard. Match requires same UTC date,
same normalized mode (or blank), and time within +/- window minutes.

Output:
    Markdown table to stdout AND results.md next to the input CSV
    (or in CWD when reading stdin).
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path

from log_utils import load_adif_file, norm_mode, time_to_mins


WINDOW_DEFAULT = 60  # minutes


def _norm_date(s: str) -> str:
    """Accept YYYY-MM-DD, YYYY/MM/DD, or YYYYMMDD. Return YYYYMMDD."""
    d = re.sub(r"[^0-9]", "", s.strip())
    if len(d) != 8:
        raise ValueError(f"Bad date: {s!r}")
    return d


def _norm_time(s: str) -> str:
    """Accept HH:MM, HHMM, HH:MM:SS, HHMMSS. Return HHMM."""
    t = re.sub(r"[^0-9]", "", s.strip())
    if len(t) < 3:
        raise ValueError(f"Bad time: {s!r}")
    return (t + "0000")[:4]


def load_eqsl_rows(reader) -> list[dict]:
    """Read a CSV reader into [{'CALL','DATE','TIME','MODE'}]. Header optional;
    mode column optional (treated as wildcard when blank/missing)."""
    rows: list[dict] = []
    first = True
    for raw in reader:
        if not raw or all(not c.strip() for c in raw):
            continue
        cells = [c.strip() for c in raw]
        if first:
            first = False
            if cells[0].lower() in ("callsign", "call", "station"):
                continue
        if len(cells) < 3:
            continue
        call, date, time = cells[0], cells[1], cells[2]
        mode = cells[3] if len(cells) >= 4 else ""
        try:
            rows.append({
                "CALL": call.upper(),
                "DATE": _norm_date(date),
                "TIME": _norm_time(time),
                "MODE": mode,
            })
        except ValueError as exc:
            print(f"[skip] {exc}", file=sys.stderr)
    return rows


def _base_call(call: str) -> str:
    """Strip operator suffixes like /QRP, /M, /MM, /P, /AM, /N4. Keeps prefix.
    'M3TLJ/QRP' -> 'M3TLJ', 'WA8MDC/4' -> 'WA8MDC'. Prefix like 'KH6/W1AW'
    is preserved as-is (we just split on '/' and take the longest segment)."""
    c = call.upper().strip()
    if "/" not in c:
        return c
    parts = c.split("/")
    return max(parts, key=len)


def build_log_index(records: list[dict]) -> tuple[dict, dict]:
    """Index ADIF records by (CALL, QSO_DATE) and also by base-call alone.
    Returns (by_call_date, by_base_call)."""
    by_call_date: dict[tuple[str, str], list[dict]] = {}
    by_base_call: dict[str, list[dict]] = {}
    for r in records:
        call = r.get("CALL", "").upper().strip()
        date = r.get("QSO_DATE", "").strip()
        if not call:
            continue
        base = _base_call(call)
        if date:
            by_call_date.setdefault((call, date), []).append(r)
            if base != call:
                by_call_date.setdefault((base, date), []).append(r)
        by_base_call.setdefault(base, []).append(r)
    return by_call_date, by_base_call


def verify(
    eqsl_rows: list[dict],
    log_records: list[dict],
    window: int,
) -> list[dict]:
    """For each eQSL row, classify into YES / WRONG_TIME / WRONG_MODE / NOT_IN_LOG.

    YES         -- callsign + same UTC date + matching mode + time within +/-window.
    WRONG_MODE  -- callsign + date + time match, but mode differs.
    WRONG_TIME  -- callsign exists in log but not at the claimed date/time
                   (or outside window). Mode may or may not differ.
    NOT_IN_LOG  -- callsign never appears in log under any date.

    Suffix-tolerant: 'M3TLJ' in eQSL matches 'M3TLJ/QRP' in log, etc.
    Mode comparison uses log_utils.norm_mode (SSB == USB == LSB, etc.).
    Blank mode in either side acts as a wildcard.
    """
    by_call_date, by_base_call = build_log_index(log_records)
    results: list[dict] = []

    for row in eqsl_rows:
        call, date, t_str = row["CALL"], row["DATE"], row["TIME"]
        eqsl_mode = norm_mode(row.get("MODE", "")) if row.get("MODE") else ""
        base = _base_call(call)
        t_mins = time_to_mins(t_str)

        candidates = list(by_call_date.get((call, date), []))
        if base != call:
            candidates += by_call_date.get((base, date), [])

        best: dict | None = None
        best_diff: int | None = None
        best_mode_ok: bool = False

        for rec in candidates:
            rt = time_to_mins(rec.get("TIME_ON", ""))
            diff = abs(rt - t_mins)
            if diff > window:
                continue
            log_mode = norm_mode(rec.get("MODE", ""))
            mode_ok = (not eqsl_mode) or (not log_mode) or (eqsl_mode == log_mode)
            # Prefer mode-matching candidates; among those, prefer smallest delta
            better = (
                best is None
                or (mode_ok and not best_mode_ok)
                or (mode_ok == best_mode_ok and diff < (best_diff or 1 << 30))
            )
            if better:
                best, best_diff, best_mode_ok = rec, diff, mode_ok

        if best is not None and best_mode_ok:
            status = "YES"
            note = ""
        elif best is not None:
            status = "WRONG_MODE"
            note = f"log mode was {norm_mode(best.get('MODE',''))}, eQSL claims {eqsl_mode}"
        else:
            other = by_base_call.get(base, [])
            if other:
                status = "WRONG_TIME"
                nearest = sorted(other, key=lambda r: r.get("QSO_DATE", ""))[-1]
                nd = nearest.get("QSO_DATE", "")
                nt = nearest.get("TIME_ON", "")[:4]
                nm = norm_mode(nearest.get("MODE", ""))
                note = (f"nearest log entry: {nd[:4]}-{nd[4:6]}-{nd[6:]} "
                        f"{nt[:2]}:{nt[2:]} ({nm}, {len(other)} total)")
            else:
                status = "NOT_IN_LOG"
                note = "callsign never appears in log"

        results.append({
            "eqsl_call": call,
            "eqsl_date": date,
            "eqsl_time": t_str,
            "eqsl_mode": eqsl_mode,
            "status": status,
            "match_time": best.get("TIME_ON", "")[:4] if best else "",
            "match_band": best.get("BAND", "") if best else "",
            "match_mode": norm_mode(best.get("MODE", "")) if best else "",
            "rst_sent": best.get("RST_SENT", "") if best else "",
            "rst_rcvd": best.get("RST_RCVD", "") if best else "",
            "delta_min": best_diff if best is not None else "",
            "note": note,
        })
    return results


def _shift_date(yyyymmdd: str, days: int) -> str | None:
    from datetime import datetime, timedelta
    try:
        d = datetime.strptime(yyyymmdd, "%Y%m%d") + timedelta(days=days)
        return d.strftime("%Y%m%d")
    except Exception:
        return None


def render_markdown(results: list[dict]) -> str:
    lines = [
        "| Callsign | Date | Time (UTC) | Mode | Status | Log time | Log mode | Band | RST Sent | RST Rcvd | Delta min | Note |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in results:
        date_fmt = f"{r['eqsl_date'][:4]}-{r['eqsl_date'][4:6]}-{r['eqsl_date'][6:]}"
        time_fmt = f"{r['eqsl_time'][:2]}:{r['eqsl_time'][2:]}"
        log_time = f"{r['match_time'][:2]}:{r['match_time'][2:]}" if r["match_time"] else ""
        lines.append(
            f"| {r['eqsl_call']} | {date_fmt} | {time_fmt} | {r['eqsl_mode']} | "
            f"{r['status']} | {log_time} | {r['match_mode']} | {r['match_band']} | "
            f"{r['rst_sent']} | {r['rst_rcvd']} | {r['delta_min']} | {r['note']} |"
        )

    yes = sum(1 for r in results if r["status"] == "YES")
    wrong_mode = sum(1 for r in results if r["status"] == "WRONG_MODE")
    wrong_time = sum(1 for r in results if r["status"] == "WRONG_TIME")
    missing = sum(1 for r in results if r["status"] == "NOT_IN_LOG")
    total = len(results)
    lines.append("")
    lines.append(
        f"**Summary:** {total} eQSL entries -- "
        f"**{yes} confirmed** (same date + mode, time within window), "
        f"**{wrong_mode} wrong-mode**, "
        f"**{wrong_time} wrong-time** (callsign in log but not at claimed date/time), "
        f"**{missing} not in log**."
    )
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("adif", help="Path to jtdx_log.adi (or any ADIF file)")
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--csv", help="eQSL inbox CSV: callsign,date,time per row")
    src.add_argument("--stdin", action="store_true", help="Read CSV rows from stdin")
    ap.add_argument("--window", type=int, default=WINDOW_DEFAULT,
                    help=f"Match tolerance in minutes (default: {WINDOW_DEFAULT})")
    args = ap.parse_args()

    log_records = load_adif_file(args.adif)
    if not log_records:
        print(f"[error] No records parsed from {args.adif}", file=sys.stderr)
        return 2

    if args.stdin:
        eqsl_rows = load_eqsl_rows(csv.reader(sys.stdin))
        out_path = Path.cwd() / "results.md"
    else:
        with open(args.csv, newline="", encoding="utf-8-sig") as fh:
            eqsl_rows = load_eqsl_rows(csv.reader(fh))
        out_path = Path(args.csv).with_name("results.md")

    if not eqsl_rows:
        print("[error] No eQSL rows parsed", file=sys.stderr)
        return 2

    results = verify(eqsl_rows, log_records, args.window)
    md = render_markdown(results)
    print(md)
    out_path.write_text(md, encoding="utf-8")
    print(f"\n[wrote] {out_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
