"""
Ham Radio Log Scanner -- Core Library
======================================
Compare and deduplicate QSO logs from multiple sources (ADIF files, CSV,
LoTW, QRZ, ClubLog). Find intra-log duplicates, cross-log gaps, and produce
a merged/deduped master ADIF.

No GUI dependencies -- safe for CLI use.
"""
from __future__ import annotations

import os
import urllib.request
import urllib.parse
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Optional

from log_utils import (
    parse_adif_records, build_adif, load_adif_file, load_csv_records,
    load_log_file, normalize_record, infer_missing_bands,
    qso_key, qso_key_relaxed, keys_of,
    find_missing, dedupe_prefer_exact_time,
    norm_band, norm_mode, time_to_mins,
)

MatchMode = Literal['strict', 'relaxed']


# =============================================================================
#  DATA CLASSES
# =============================================================================

@dataclass
class LogSource:
    name: str                               # e.g. "HRD", "JTDX", "LoTW"
    origin: str                             # file path or "api:qrz" etc.
    records: list[dict] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.records)


@dataclass
class DupeGroup:
    key: tuple                              # the dedup key
    records: list[dict]                     # all records sharing this key
    source: str                             # LogSource.name


@dataclass
class MissingQSO:
    record: dict                            # the QSO record
    present_in: list[str]                   # source names that have it
    missing_from: list[str]                 # source names that lack it


@dataclass
class ScanReport:
    sources: list[LogSource]
    intra_dupes: dict[str, list[DupeGroup]] # source_name -> dupe groups
    missing: list[MissingQSO]              # cross-source gaps
    master_records: list[dict]             # merged/deduped master set
    match_mode: MatchMode = 'strict'
    window_min: int = 1


# =============================================================================
#  SOURCE LOADING
# =============================================================================

def load_source(name: str, path: str) -> LogSource:
    """Load a log source from a local ADIF or CSV file.
    Normalizes fields and infers missing bands from surrounding QSOs."""
    records = load_log_file(path)
    for rec in records:
        normalize_record(rec)
    filled = infer_missing_bands(records)
    if filled:
        print(f"  [{name}] Inferred band for {filled} records from surrounding QSOs")
    return LogSource(name=name, origin=path, records=records)


def load_source_from_records(name: str, origin: str,
                             records: list[dict]) -> LogSource:
    """Wrap pre-fetched records into a LogSource."""
    for rec in records:
        normalize_record(rec)
    return LogSource(name=name, origin=origin, records=records)


# =============================================================================
#  API CONNECTORS
# =============================================================================

def fetch_lotw(callsign: str, password: str,
               qso_qslsince: str = '') -> list[dict]:
    """Fetch QSO records from LoTW ADIF download endpoint.

    Args:
        callsign: LoTW login callsign
        password: LoTW password
        qso_qslsince: Optional date filter (YYYY-MM-DD)

    Returns:
        List of ADIF record dicts.
    """
    params = {
        'login': callsign,
        'password': password,
        'qso_query': '1',
        'qso_qsl': 'no',
        'qso_qsldetail': 'no',
    }
    if qso_qslsince:
        params['qso_qslsince'] = qso_qslsince

    url = 'https://lotw.arrl.org/lotwuser/lotwreport.adi?' + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=60) as resp:
            content = resp.read().decode('utf-8', errors='replace')
        if 'ARRL Logbook' not in content and '<EOH>' not in content.upper():
            raise ValueError(f"LoTW login failed or unexpected response: {content[:200]}")
        return parse_adif_records(content)
    except Exception as exc:
        print(f"[LOTW] Fetch error: {exc}")
        return []


def fetch_clublog(callsign: str, email: str, password: str,
                  api_key: str) -> list[dict]:
    """Fetch QSO records from ClubLog ADIF export.

    Args:
        callsign: Your callsign
        email: ClubLog account email
        password: ClubLog password
        api_key: ClubLog application API key

    Returns:
        List of ADIF record dicts.
    """
    params = {
        'call': callsign,
        'email': email,
        'password': password,
        'api': api_key,
    }
    url = 'https://clublog.org/getlog.php'
    try:
        data = urllib.parse.urlencode(params).encode('utf-8')
        req = urllib.request.Request(url, data=data, method='POST')
        with urllib.request.urlopen(req, timeout=60) as resp:
            content = resp.read().decode('utf-8', errors='replace')
        if '<EOH>' not in content.upper():
            raise ValueError(f"ClubLog error: {content[:200]}")
        return parse_adif_records(content)
    except Exception as exc:
        print(f"[CLUBLOG] Fetch error: {exc}")
        return []


def fetch_qrz(api_key: str) -> list[dict]:
    """Fetch QSO records from QRZ.com logbook API.

    Args:
        api_key: QRZ.com API key (from My Logbook > Settings > API)

    Returns:
        List of ADIF record dicts.
    """
    import requests
    all_records: list[dict] = []
    try:
        # QRZ logbook API uses ADIF fetch
        url = 'https://logbook.qrz.com/api'
        # First get session key
        resp = requests.post(url, data={
            'KEY': api_key,
            'ACTION': 'FETCH',
            'OPTION': 'TYPE:ADIF',
        }, timeout=60)
        content = resp.text
        if '<EOH>' in content.upper():
            all_records = parse_adif_records(content)
        elif 'RESULT=OK' in content.upper():
            # May need to parse differently; QRZ returns records in batches
            all_records = parse_adif_records(content)
        else:
            print(f"[QRZ] Unexpected response: {content[:200]}")
    except Exception as exc:
        print(f"[QRZ] Fetch error: {exc}")
    return all_records


def fetch_eqsl(user: str, password: str) -> list[dict]:
    """Fetch QSO records from eQSL.cc inbox (confirmed QSLs).

    Args:
        user: eQSL username (callsign)
        password: eQSL password

    Returns:
        List of ADIF record dicts.
    """
    params = {
        'UserName': user,
        'Password': password,
        'RcvdSince': '01011990',
        'ConfirmedOnly': '1',
    }
    url = 'https://www.eqsl.cc/qslcard/DownloadInBox.cfm?' + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=60) as resp:
            content = resp.read().decode('utf-8', errors='replace')
        if '<EOH>' in content.upper():
            return parse_adif_records(content)
        elif 'error' in content.lower():
            print(f"[EQSL] Error: {content[:200]}")
        return []
    except Exception as exc:
        print(f"[EQSL] Fetch error: {exc}")
        return []


# =============================================================================
#  INTRA-LOG DUPLICATE DETECTION
# =============================================================================

def find_intra_dupes(source: LogSource,
                     mode: MatchMode = 'strict') -> list[DupeGroup]:
    """Find duplicate QSOs within a single log source.

    strict: exact match on (call, date, time_hhmm, band, mode)
    relaxed: match on (call, date, band, mode) ignoring time
    """
    key_fn = qso_key if mode == 'strict' else qso_key_relaxed
    groups: dict[tuple, list[dict]] = defaultdict(list)

    for rec in source.records:
        k = key_fn(rec)
        groups[k].append(rec)

    return [
        DupeGroup(key=k, records=recs, source=source.name)
        for k, recs in groups.items()
        if len(recs) > 1
    ]


# =============================================================================
#  CROSS-LOG GAP DETECTION
# =============================================================================

def find_inter_gaps(sources: list[LogSource],
                    mode: MatchMode = 'strict',
                    window_min: int = 1) -> list[MissingQSO]:
    """Find QSOs present in some sources but missing from others.

    Uses fuzzy matching (find_missing) which handles blank bands/modes as
    wildcards and applies a time window for near-match detection.

    For each source, finds records not present in each other source,
    then consolidates into MissingQSO entries.
    """
    if len(sources) < 2:
        return []

    # Build the master deduped record list with source tags
    master = dedupe_prefer_exact_time(
        [dict(r, _src=src.name) for src in sources for r in src.records],
        window=window_min,
    )

    # For each master record, determine which sources have it
    # by checking each source's records with find_missing
    source_names = [s.name for s in sources]

    # Build fuzzy lookup per source: (call, date, mode) -> [time_mins]
    # and mode-agnostic: (call, date) -> [time_mins]
    source_fuzzy: dict[str, dict[tuple, list[int]]] = {}
    source_nomode: dict[str, dict[tuple, list[int]]] = {}
    for src in sources:
        fuzzy: dict[tuple, list[int]] = {}
        nomode: dict[tuple, list[int]] = {}
        for rec in src.records:
            call = rec.get('CALL', '').upper().strip()
            date = rec.get('QSO_DATE', '').strip()
            md = norm_mode(rec.get('MODE', ''))
            t = time_to_mins(rec.get('TIME_ON', ''))
            fuzzy.setdefault((call, date, md), []).append(t)
            nomode.setdefault((call, date), []).append(t)
        source_fuzzy[src.name] = fuzzy
        source_nomode[src.name] = nomode

    def _is_in_source(rec: dict, src_name: str) -> bool:
        call = rec.get('CALL', '').upper().strip()
        date = rec.get('QSO_DATE', '').strip()
        md = norm_mode(rec.get('MODE', ''))
        band = norm_band(rec.get('BAND', ''))
        t = time_to_mins(rec.get('TIME_ON', ''))
        fuzzy = source_fuzzy[src_name]
        nomode = source_nomode[src_name]
        # Mode-specific match
        if any(abs(t - tt) <= window_min for tt in fuzzy.get((call, date, md), [])):
            return True
        # Blank mode/band wildcard match
        if not md or not band:
            if any(abs(t - tt) <= window_min for tt in nomode.get((call, date), [])):
                return True
        return False

    result: list[MissingQSO] = []
    for rec in master:
        present = [n for n in source_names if _is_in_source(rec, n)]
        missing = [n for n in source_names if n not in present]
        if missing:
            result.append(MissingQSO(
                record=rec,
                present_in=present,
                missing_from=missing,
            ))

    return result


# =============================================================================
#  MASTER MERGE
# =============================================================================

def build_master(sources: list[LogSource],
                 window_min: int = 1) -> list[dict]:
    """Merge all sources into a single deduped master record list."""
    all_records: list[dict] = []
    for src in sources:
        for rec in src.records:
            tagged = dict(rec)
            tagged['_SOURCE'] = src.name
            all_records.append(tagged)

    return dedupe_prefer_exact_time(all_records, window=window_min)


def export_adif(records: list[dict], path: str) -> int:
    """Write records to an ADIF file. Returns record count."""
    header = (
        f"Log Scanner merged export\n"
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"Records: {len(records)}\n"
        f"<ADIF_VER:5>3.1.4\n"
        f"<PROGRAMID:11>Log Scanner\n"
        f"<PROGRAMVERSION:3>1.0"
    )
    content = build_adif(records, header)
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(content)
    return len(records)


# =============================================================================
#  FULL SCAN
# =============================================================================

def run_scan(sources: list[LogSource],
             mode: MatchMode = 'strict',
             window_min: int = 1) -> ScanReport:
    """Run a complete scan: intra-dupes, inter-gaps, and master merge."""
    intra: dict[str, list[DupeGroup]] = {}
    for src in sources:
        dupes = find_intra_dupes(src, mode=mode)
        intra[src.name] = dupes

    gaps = find_inter_gaps(sources, mode=mode, window_min=window_min)
    master = build_master(sources, window_min=window_min)

    return ScanReport(
        sources=sources,
        intra_dupes=intra,
        missing=gaps,
        master_records=master,
        match_mode=mode,
        window_min=window_min,
    )


# =============================================================================
#  REPORT GENERATION
# =============================================================================

def generate_report(report: ScanReport) -> str:
    """Generate a human-readable text report from a ScanReport."""
    lines: list[str] = []
    lines.append('=' * 70)
    lines.append('  HAM RADIO LOG SCANNER REPORT')
    lines.append(f'  Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    lines.append(f'  Match mode: {report.match_mode}  |  Window: {report.window_min} min')
    lines.append('=' * 70)

    # -- Source summary --
    lines.append('')
    lines.append('  SOURCES')
    lines.append('  ' + '-' * 50)
    total_qsos = 0
    for src in report.sources:
        n_dupes = sum(len(dg.records) - 1 for dg in report.intra_dupes.get(src.name, []))
        lines.append(f'  {src.name:<20s}  {src.count:>6d} QSOs  '
                     f'{n_dupes:>4d} dupes  ({src.origin})')
        total_qsos += src.count
    lines.append(f'  {"TOTAL":<20s}  {total_qsos:>6d} QSOs')
    lines.append(f'  {"UNIQUE (merged)":<20s}  {len(report.master_records):>6d} QSOs')

    # -- Intra-log dupes --
    lines.append('')
    lines.append('  INTRA-LOG DUPLICATES')
    lines.append('  ' + '-' * 50)
    any_dupes = False
    for src in report.sources:
        dupes = report.intra_dupes.get(src.name, [])
        if not dupes:
            continue
        any_dupes = True
        total_extra = sum(len(dg.records) - 1 for dg in dupes)
        lines.append(f'  {src.name}: {len(dupes)} dupe groups ({total_extra} extra records)')
        for dg in dupes[:20]:  # limit display
            call = dg.records[0].get('CALL', '?')
            date = dg.records[0].get('QSO_DATE', '?')
            band = dg.records[0].get('BAND', '?')
            mode = dg.records[0].get('MODE', '?')
            times = [r.get('TIME_ON', '?') for r in dg.records]
            lines.append(f'    {call:<12s} {date} {band:<6s} {mode:<6s} '
                         f'x{len(dg.records)} times: {", ".join(times)}')
        if len(dupes) > 20:
            lines.append(f'    ... and {len(dupes) - 20} more dupe groups')
    if not any_dupes:
        lines.append('  No duplicates found in any source.')

    # -- Cross-log gaps --
    lines.append('')
    lines.append('  CROSS-LOG GAPS')
    lines.append('  ' + '-' * 50)
    if report.missing:
        # Gap matrix
        src_names = [s.name for s in report.sources]
        gap_matrix: dict[tuple[str, str], int] = defaultdict(int)
        for m in report.missing:
            for missing_name in m.missing_from:
                for present_name in m.present_in:
                    gap_matrix[(present_name, missing_name)] += 1

        # Header
        col_w = max(len(n) for n in src_names) + 2
        header = f'  {"Missing from ->":>{col_w}}'
        for name in src_names:
            header += f'  {name:>{col_w}}'
        lines.append(header)

        for present in src_names:
            row = f'  {"In " + present:>{col_w}}'
            for missing in src_names:
                if present == missing:
                    row += f'  {"--":>{col_w}}'
                else:
                    count = gap_matrix.get((present, missing), 0)
                    row += f'  {count:>{col_w}d}'
            lines.append(row)

        lines.append('')
        lines.append(f'  Total gap records: {len(report.missing)}')

        # Sample gaps
        lines.append('')
        lines.append('  Sample gaps (first 30):')
        for mq in report.missing[:30]:
            rec = mq.record
            call = rec.get('CALL', '?')
            date = rec.get('QSO_DATE', '?')
            time = rec.get('TIME_ON', '?')
            band = rec.get('BAND', '?')
            mode = rec.get('MODE', '?')
            present = ', '.join(mq.present_in)
            missing = ', '.join(mq.missing_from)
            lines.append(f'    {call:<12s} {date} {time:<6s} {band:<6s} {mode:<6s} '
                         f'IN: {present}  MISSING: {missing}')
        if len(report.missing) > 30:
            lines.append(f'    ... and {len(report.missing) - 30} more gaps')
    else:
        lines.append('  All sources are in sync -- no gaps found.')

    lines.append('')
    lines.append('=' * 70)
    return '\n'.join(lines)
