#!/usr/bin/env python3
"""
Ham Radio Log Scanner -- CLI
=============================
Compare and deduplicate QSO logs from multiple sources.

Usage examples:
  python log_scanner_cli.py --files hrd.adi jtdx.adi lotw.adi
  python log_scanner_cli.py --files hrd.adi --lotw-user CALL --lotw-pass XXXX
  python log_scanner_cli.py --files hrd.adi jtdx.adi --mode relaxed --export merged.adi
"""
from __future__ import annotations

import argparse
import os
import sys

from log_scanner import (
    LogSource, ScanReport,
    load_source, load_source_from_records,
    fetch_lotw, fetch_clublog, fetch_qrz, fetch_eqsl,
    run_scan, export_adif, generate_report,
)


def _auto_name(path: str) -> str:
    """Derive a short source name from a file path."""
    base = os.path.splitext(os.path.basename(path))[0]
    # Common patterns
    lower = base.lower()
    if 'hrd' in lower or 'exportall' in lower:
        return 'HRD'
    if 'jtdx' in lower or 'wsjtx' in lower:
        return 'JTDX'
    if 'lotw' in lower:
        return 'LoTW'
    if 'qrz' in lower:
        return 'QRZ'
    if 'clublog' in lower:
        return 'ClubLog'
    if 'eqsl' in lower:
        return 'eQSL'
    if 'gridtracker' in lower or 'gt2' in lower:
        return 'GT2'
    return base[:20]


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Ham Radio Log Scanner -- compare and deduplicate QSO logs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            'Examples:\n'
            '  %(prog)s --files hrd.adi jtdx.adi lotw.adi\n'
            '  %(prog)s --files hrd.adi --lotw-user CALL --lotw-pass SECRET\n'
            '  %(prog)s --files *.adi --mode relaxed --export merged.adi\n'
        ),
    )
    parser.add_argument('--files', nargs='+', metavar='FILE',
                        help='ADIF or CSV log files to compare')
    parser.add_argument('--names', nargs='+', metavar='NAME',
                        help='Optional names for each file (must match --files count)')

    # API sources
    api_group = parser.add_argument_group('API sources')
    api_group.add_argument('--lotw-user', metavar='CALL',
                           help='LoTW login callsign')
    api_group.add_argument('--lotw-pass', metavar='PASS',
                           help='LoTW password')
    api_group.add_argument('--qrz-key', metavar='KEY',
                           help='QRZ.com logbook API key')
    api_group.add_argument('--eqsl-user', metavar='USER',
                           help='eQSL.cc username (callsign)')
    api_group.add_argument('--eqsl-pass', metavar='PASS',
                           help='eQSL.cc password')
    api_group.add_argument('--clublog-call', metavar='CALL',
                           help='ClubLog callsign')
    api_group.add_argument('--clublog-email', metavar='EMAIL',
                           help='ClubLog account email')
    api_group.add_argument('--clublog-pass', metavar='PASS',
                           help='ClubLog password')
    api_group.add_argument('--clublog-api', metavar='KEY',
                           help='ClubLog application API key')

    # Scan options
    scan_group = parser.add_argument_group('Scan options')
    scan_group.add_argument('--mode', choices=['strict', 'relaxed'],
                            default='strict',
                            help='Match mode: strict (call+date+time+band+mode) '
                                 'or relaxed (call+date+band+mode, ignores time)')
    scan_group.add_argument('--window', type=int, default=1, metavar='MIN',
                            help='Time window in minutes for fuzzy matching (default: 1)')

    # Output
    out_group = parser.add_argument_group('Output')
    out_group.add_argument('--export', metavar='PATH',
                           help='Export merged/deduped master ADIF to this file')
    out_group.add_argument('--quiet', action='store_true',
                           help='Suppress the full report; only show summary line')

    args = parser.parse_args()

    if not args.files and not args.lotw_user and not args.clublog_call and not args.qrz_key and not args.eqsl_user:
        parser.error('At least one source required (--files, --lotw-user, or --clublog-call)')

    # -- Load sources --
    sources: list[LogSource] = []

    if args.files:
        names = args.names or []
        for i, path in enumerate(args.files):
            if not os.path.isfile(path):
                print(f"[ERROR] File not found: {path}", file=sys.stderr)
                sys.exit(1)
            name = names[i] if i < len(names) else _auto_name(path)
            print(f"Loading {name} from {path} ...", file=sys.stderr)
            src = load_source(name, path)
            print(f"  {src.count} records", file=sys.stderr)
            sources.append(src)

    if args.lotw_user and args.lotw_pass:
        print("Fetching from LoTW ...", file=sys.stderr)
        records = fetch_lotw(args.lotw_user, args.lotw_pass)
        src = load_source_from_records('LoTW', 'api:lotw', records)
        print(f"  {src.count} records", file=sys.stderr)
        sources.append(src)

    if args.qrz_key:
        print("Fetching from QRZ ...", file=sys.stderr)
        records = fetch_qrz(args.qrz_key)
        src = load_source_from_records('QRZ', 'api:qrz', records)
        print(f"  {src.count} records", file=sys.stderr)
        sources.append(src)

    if args.eqsl_user and args.eqsl_pass:
        print("Fetching from eQSL ...", file=sys.stderr)
        records = fetch_eqsl(args.eqsl_user, args.eqsl_pass)
        src = load_source_from_records('eQSL', 'api:eqsl', records)
        print(f"  {src.count} records", file=sys.stderr)
        sources.append(src)

    if args.clublog_call and args.clublog_email and args.clublog_pass and args.clublog_api:
        print("Fetching from ClubLog ...", file=sys.stderr)
        records = fetch_clublog(
            args.clublog_call, args.clublog_email,
            args.clublog_pass, args.clublog_api,
        )
        src = load_source_from_records('ClubLog', 'api:clublog', records)
        print(f"  {src.count} records", file=sys.stderr)
        sources.append(src)

    if len(sources) < 1:
        print("[ERROR] No sources loaded.", file=sys.stderr)
        sys.exit(1)

    # -- Run scan --
    print(f"\nScanning {len(sources)} sources (mode={args.mode}, window={args.window}min) ...",
          file=sys.stderr)
    report = run_scan(sources, mode=args.mode, window_min=args.window)

    # -- Output --
    if not args.quiet:
        print(generate_report(report))
    else:
        total_dupes = sum(
            sum(len(dg.records) - 1 for dg in dupes)
            for dupes in report.intra_dupes.values()
        )
        print(f"Sources: {len(sources)}  |  "
              f"Unique: {len(report.master_records)}  |  "
              f"Dupes: {total_dupes}  |  "
              f"Gaps: {len(report.missing)}")

    if args.export:
        n = export_adif(report.master_records, args.export)
        print(f"\nExported {n} records to {args.export}", file=sys.stderr)


if __name__ == '__main__':
    main()
