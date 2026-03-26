"""
Ham Radio Log Utilities
=======================
Shared ADIF parsing, writing, normalization, and dedup functions.
No GUI dependencies — safe for CLI and headless use.
"""
from __future__ import annotations

import csv
import re
from datetime import datetime
from typing import Optional


# =============================================================================
#  ADIF PARSER & WRITER
# =============================================================================

def parse_adif_records(content: str) -> list[dict]:
    """
    Parse raw ADIF text into a list of {FIELD: value} dicts.
    Handles <FIELDNAME:LENGTH>value format. Skips header before <EOH>.
    """
    eoh = re.search(r'<EOH>', content, re.IGNORECASE)
    if eoh:
        content = content[eoh.end():]

    tag_re = re.compile(r'<(\w+)(?::(\d+)(?::\w+)?)?>',  re.IGNORECASE)
    records: list[dict] = []
    current: dict = {}
    pos = 0

    while pos < len(content):
        m = tag_re.search(content, pos)
        if not m:
            break
        name     = m.group(1).upper()
        length_s = m.group(2)
        tag_end  = m.end()

        if name == 'EOR':
            if current:
                records.append(current)
                current = {}
            pos = tag_end
        elif name == 'EOH':
            pos = tag_end
        elif length_s is not None:
            n = int(length_s)
            current[name] = content[tag_end:tag_end + n].strip()
            pos = tag_end + n
        else:
            pos = tag_end

    return records


def build_adif(records: list[dict], header: str = '') -> str:
    """Build an ADIF string from a list of field dicts.
    Fields starting with '_' are internal/display-only and are skipped."""
    parts: list[str] = []
    if header:
        parts.append(header + '\n')
    parts.append('<EOH>\n')
    for rec in records:
        for field, value in rec.items():
            if value and not field.startswith('_'):
                s = str(value)
                parts.append(f'<{field}:{len(s)}>{s} ')
        parts.append('<EOR>\n')
    return ''.join(parts)


def load_adif_file(path: str) -> list[dict]:
    """Load an ADIF file and return a list of record dicts."""
    for enc in ('utf-8-sig', 'latin-1'):
        try:
            with open(path, encoding=enc, errors='replace') as fh:
                content = fh.read()
            return parse_adif_records(content)
        except Exception:
            continue
    return []


def load_csv_records(path: str) -> list[dict]:
    """Load a CSV log file into ADIF-style record dicts.

    Flexible column detection: CALL/CALLSIGN/STATION_CALLSIGN,
    BAND, MODE/SUBMODE, QSO_DATE/DATE, TIME_ON/TIME.
    """
    records: list[dict] = []
    try:
        with open(path, newline='', encoding='utf-8-sig') as fh:
            reader = csv.DictReader(fh)
            if reader.fieldnames is None:
                return records
            norm = {f.strip().upper(): f for f in reader.fieldnames}
            call_col = norm.get('CALL') or norm.get('CALLSIGN') or norm.get('STATION_CALLSIGN')
            band_col = norm.get('BAND')
            mode_col = norm.get('MODE') or norm.get('SUBMODE')
            date_col = norm.get('QSO_DATE') or norm.get('DATE')
            time_col = norm.get('TIME_ON') or norm.get('TIME')
            freq_col = norm.get('FREQ')
            rst_s_col = norm.get('RST_SENT')
            rst_r_col = norm.get('RST_RCVD')
            grid_col = norm.get('GRIDSQUARE') or norm.get('GRID')
            if call_col is None:
                return records
            for row in reader:
                call = row.get(call_col, '').strip().upper()
                if not call:
                    continue
                rec: dict = {'CALL': call}
                if band_col:
                    rec['BAND'] = row.get(band_col, '').strip()
                if mode_col:
                    rec['MODE'] = row.get(mode_col, '').strip()
                if date_col:
                    rec['QSO_DATE'] = row.get(date_col, '').strip()
                if time_col:
                    rec['TIME_ON'] = row.get(time_col, '').strip()
                if freq_col:
                    rec['FREQ'] = row.get(freq_col, '').strip()
                if rst_s_col:
                    rec['RST_SENT'] = row.get(rst_s_col, '').strip()
                if rst_r_col:
                    rec['RST_RCVD'] = row.get(rst_r_col, '').strip()
                if grid_col:
                    rec['GRIDSQUARE'] = row.get(grid_col, '').strip()
                records.append(rec)
    except Exception as exc:
        print(f"[LOG_UTILS] Error reading CSV {path}: {exc}")
    return records


def load_log_file(path: str) -> list[dict]:
    """Auto-detect format (ADIF or CSV) and load records."""
    ext = path.lower().rsplit('.', 1)[-1] if '.' in path else ''
    if ext in ('adi', 'adif'):
        return load_adif_file(path)
    elif ext == 'csv':
        return load_csv_records(path)
    # Try ADIF first, fall back to CSV
    records = load_adif_file(path)
    return records if records else load_csv_records(path)


# =============================================================================
#  BAND / MODE NORMALIZATION
# =============================================================================

BAND_ALIASES: dict[str, str] = {
    # Standard names (pass-through, lowercased)
    '160m': '160m', '80m': '80m', '60m': '60m', '40m': '40m',
    '30m': '30m',   '20m': '20m', '17m': '17m', '15m': '15m',
    '12m': '12m',   '10m': '10m', '6m':  '6m',  '4m':  '4m',
    '2m': '2m', '1.25m': '1.25m', '70cm': '70cm', '33cm': '33cm',
    '23cm': '23cm',
    # Frequency-based identifiers (MHz)
    '1.8': '160m', '1.8mhz': '160m',
    '3.5': '80m',  '3.5mhz': '80m',  '3.7': '80m',  '3.7mhz': '80m',
    '5':   '60m',  '5mhz':   '60m',
    '7':   '40m',  '7mhz':   '40m',  '7.0': '40m',  '7.1': '40m',
    '10':  '30m',  '10mhz':  '30m',  '10.1': '30m',
    '14':  '20m',  '14mhz':  '20m',  '14.0': '20m', '14.2': '20m',
    '18':  '17m',  '18mhz':  '17m',  '18.1': '17m',
    '21':  '15m',  '21mhz':  '15m',  '21.0': '15m',
    '24':  '12m',  '24mhz':  '12m',  '24.9': '12m',
    '28':  '10m',  '28mhz':  '10m',  '28.0': '10m', '29': '10m',
    '50':  '6m',   '50mhz':  '6m',
    '144': '2m',   '144mhz': '2m',   '145': '2m',
    '430': '70cm', '430mhz': '70cm', '432': '70cm', '440': '70cm',
    '1240': '23cm', '1296': '23cm',
}

MODE_ALIASES: dict[str, str] = {
    # SSB
    'usb': 'SSB', 'lsb': 'SSB', 'ssb': 'SSB',
    # CW
    'cw': 'CW', 'cw-r': 'CW',
    # AM / FM
    'am': 'AM', 'fm': 'FM',
    # Digital — FT8 / FT4
    'ft8': 'FT8', 'ft4': 'FT4',
    # JT modes
    'jt65': 'JT65', 'jt65a': 'JT65', 'jt65b': 'JT65', 'jt65c': 'JT65',
    'jt9': 'JT9', 'jt9-1': 'JT9',
    # WSPR
    'wspr': 'WSPR',
    # PSK
    'psk':    'PSK31', 'psk31': 'PSK31', 'bpsk31': 'PSK31', 'psk-31': 'PSK31',
    'psk63':  'PSK63', 'bpsk63': 'PSK63',
    'psk125': 'PSK125',
    # RTTY
    'rtty': 'RTTY', 'fsk': 'RTTY', 'afsk': 'RTTY',
    # Other digital
    'olivia': 'OLIVIA',
    'mfsk': 'MFSK', 'mfsk16': 'MFSK', 'mfsk8': 'MFSK',
    'thor': 'THOR',
    'hell': 'HELL', 'hellschreiber': 'HELL',
    'sstv': 'SSTV',
    'digi': 'DIGI', 'data': 'DIGI',
    # JS8
    'js8': 'JS8', 'js8call': 'JS8',
}


def norm_band(band: str) -> str:
    """Normalize a band string to ADIF standard (e.g. '7mhz' -> '40m')."""
    return BAND_ALIASES.get(band.lower().strip(), band.lower().strip())


def norm_mode(mode: str) -> str:
    """Normalize a mode string to canonical form (e.g. 'USB' -> 'SSB')."""
    return MODE_ALIASES.get(mode.lower().strip(), mode.upper().strip())


# =============================================================================
#  QSO KEY & TIME UTILITIES
# =============================================================================

def qso_key(rec: dict) -> tuple:
    """Dedup key: (callsign, YYYYMMDD, HHMM, normalized-band, normalized-mode)."""
    call = rec.get('CALL', '').upper().strip()
    date = rec.get('QSO_DATE', '').strip()
    t = rec.get('TIME_ON', '0000')[:4].strip()
    band = norm_band(rec.get('BAND', ''))
    mode = norm_mode(rec.get('MODE', ''))
    return (call, date, t, band, mode)


def qso_key_relaxed(rec: dict) -> tuple:
    """Relaxed dedup key: (callsign, YYYYMMDD, normalized-band, normalized-mode).
    Ignores time — treats same call/date/band/mode as the same QSO."""
    call = rec.get('CALL', '').upper().strip()
    date = rec.get('QSO_DATE', '').strip()
    band = norm_band(rec.get('BAND', ''))
    mode = norm_mode(rec.get('MODE', ''))
    return (call, date, band, mode)


def keys_of(records: list[dict]) -> set[tuple]:
    return {qso_key(r) for r in records}


def time_to_mins(time_str: str) -> int:
    """Convert HHMM string to total minutes since midnight."""
    try:
        s = (time_str + '0000')[:4]
        return int(s[:2]) * 60 + int(s[2:4])
    except Exception:
        return 0


def date_to_ord(date_str: str) -> Optional[int]:
    """Convert YYYYMMDD to an integer day-ordinal for cross-day arithmetic."""
    try:
        return datetime.strptime(date_str.strip(), '%Y%m%d').toordinal()
    except Exception:
        return None


# =============================================================================
#  MATCHING & DEDUP
# =============================================================================

def is_round_time(rec: dict) -> bool:
    """True if TIME_ON has :00 seconds or no seconds component at all."""
    t = rec.get('TIME_ON', '')
    if len(t) <= 4:
        return True
    return t[4:6] == '00'


def find_missing(source_records: list[dict], target_records: list[dict],
                 window: int = 1) -> list[dict]:
    """
    Return records from source that are genuinely absent from target.

    A record is considered present if target contains a record with the same
    CALL, QSO_DATE, normalized MODE, and TIME_ON within +/- window minutes.
    Band is intentionally excluded so minor band-label discrepancies between
    logging services do not create phantom missing records.

    When either side has a blank/missing mode, mode comparison is skipped
    (treated as wildcard).

    An exact key match (qso_key) is also accepted as present.
    """
    exact_keys: set = keys_of(target_records)

    # Fuzzy lookup: (call, date, mode) -> [time_mins, ...]
    # Also build a mode-agnostic index for blank-mode matching
    fuzzy: dict[tuple, list[int]] = {}
    fuzzy_nomode: dict[tuple, list[int]] = {}
    for rec in target_records:
        call = rec.get('CALL', '').upper().strip()
        date = rec.get('QSO_DATE', '').strip()
        mode = norm_mode(rec.get('MODE', ''))
        t = time_to_mins(rec.get('TIME_ON', ''))
        fuzzy.setdefault((call, date, mode), []).append(t)
        fuzzy_nomode.setdefault((call, date), []).append(t)

    result: list[dict] = []
    for rec in source_records:
        if qso_key(rec) in exact_keys:
            continue
        call = rec.get('CALL', '').upper().strip()
        date = rec.get('QSO_DATE', '').strip()
        mode = norm_mode(rec.get('MODE', ''))
        t = time_to_mins(rec.get('TIME_ON', ''))
        # Try mode-specific match first
        if any(abs(t - tt) <= window for tt in fuzzy.get((call, date, mode), [])):
            continue
        # If either side has blank mode, try mode-agnostic match
        if not mode or not rec.get('MODE', '').strip():
            if any(abs(t - tt) <= window for tt in fuzzy_nomode.get((call, date), [])):
                continue
        result.append(rec)
    return result


def _bands_match(b1: str, b2: str) -> bool:
    """True if bands match, treating blank as wildcard."""
    if not b1 or not b2:
        return True
    return b1 == b2


def _modes_match(m1: str, m2: str) -> bool:
    """True if modes match, treating blank as wildcard."""
    if not m1 or not m2:
        return True
    return m1 == m2


def dedupe_prefer_exact_time(records: list[dict], window: int = 1) -> list[dict]:
    """
    Collapse near-duplicate records (same CALL + QSO_DATE + compatible BAND +
    compatible MODE, TIME_ON within +/- window minutes) down to one entry.

    Blank/missing band or mode is treated as a wildcard (matches anything).
    When merging, the record with more complete fields wins. Among equally
    complete records, prefers non-rounded seconds over :00 seconds.

    Uses dict-based lookup for O(n) average performance.
    """
    # Index: (call, date) -> list of (time_mins, band, mode, result_index)
    _idx: dict[tuple[str, str], list[tuple[int, str, str, int]]] = {}
    result: list[dict] = []

    for rec in records:
        call = rec.get('CALL', '').upper().strip()
        date = rec.get('QSO_DATE', '').strip()
        band = norm_band(rec.get('BAND', ''))
        mode = norm_mode(rec.get('MODE', ''))
        t = time_to_mins(rec.get('TIME_ON', ''))

        # Search candidates via index (only same call+date)
        matched = None
        for (ct, cb, cm, ri) in _idx.get((call, date), []):
            if (_bands_match(cb, band)
                    and _modes_match(cm, mode)
                    and abs(ct - t) <= window):
                matched = ri
                break

        if matched is None:
            ri = len(result)
            result.append(rec)
            _idx.setdefault((call, date), []).append((t, band, mode, ri))
        else:
            # Merge: fill in blank fields from the other record
            existing = result[matched]
            e_band = norm_band(existing.get('BAND', ''))
            # Prefer the record with band populated
            rec_has_more = bool(band) and not bool(e_band)
            existing_has_more = bool(e_band) and not bool(band)
            if rec_has_more:
                merged = dict(rec)
                for k, v in existing.items():
                    if v and not merged.get(k):
                        merged[k] = v
                result[matched] = merged
            elif existing_has_more:
                for k, v in rec.items():
                    if v and not existing.get(k):
                        existing[k] = v
            elif is_round_time(existing) and not is_round_time(rec):
                merged = dict(rec)
                for k, v in existing.items():
                    if v and not merged.get(k):
                        merged[k] = v
                result[matched] = merged

    return result


def near_dupe_indices(missing: list[dict], target: list[dict],
                      window: int = 15) -> dict[int, str]:
    """
    Return {index: reason_string} for records in `missing` that likely already
    exist in `target` under a slightly different timestamp, band label, or mode.

    Detection tiers:
      Tier 1 -- same CALL + DATE + norm BAND + norm MODE, TIME_ON within +/- window min.
      Tier 2 -- same CALL + norm BAND + norm MODE, adjacent date, combined time <= window.
      Tier 3 -- same CALL + DATE, TIME_ON within +/- 5 min regardless of band/mode.
    """
    tier1: dict[tuple, list[int]] = {}
    tier2: dict[tuple, list[tuple[int, int]]] = {}
    tier3: dict[tuple, list[int]] = {}

    for rec in target:
        call = rec.get('CALL', '').upper().strip()
        date = rec.get('QSO_DATE', '').strip()
        band = norm_band(rec.get('BAND', ''))
        mode = norm_mode(rec.get('MODE', ''))
        t = time_to_mins(rec.get('TIME_ON', ''))
        dord = date_to_ord(date)

        tier1.setdefault((call, date, band, mode), []).append(t)
        if dord is not None:
            tier2.setdefault((call, band, mode), []).append((dord, t))
        tier3.setdefault((call, date), []).append(t)

    result: dict[int, str] = {}

    for i, rec in enumerate(missing):
        call = rec.get('CALL', '').upper().strip()
        date = rec.get('QSO_DATE', '').strip()
        band = norm_band(rec.get('BAND', ''))
        mode = norm_mode(rec.get('MODE', ''))
        t = time_to_mins(rec.get('TIME_ON', ''))
        dord = date_to_ord(date)

        # Tier 1
        best: Optional[int] = None
        for tt in tier1.get((call, date, band, mode), []):
            diff = abs(t - tt)
            if diff <= window and (best is None or diff < best):
                best = diff
        if best is not None:
            result[i] = f'Near-dupe: same call/band/mode, time +/-{best}min'
            continue

        # Tier 2: UTC midnight boundary
        if dord is not None:
            for (td_ord, tt) in tier2.get((call, band, mode), []):
                if abs(td_ord - dord) == 1:
                    if td_ord > dord:
                        diff = abs(t - (tt + 1440))
                    else:
                        diff = abs((t + 1440) - tt)
                    if diff <= window:
                        result[i] = f'Near-dupe: UTC date boundary (+/-{diff}min)'
                        break
            if i in result:
                continue

        # Tier 3: same call/date, time very close, band/mode differs
        for tt in tier3.get((call, date), []):
            diff = abs(t - tt)
            if diff <= 5:
                orig_band = rec.get('BAND', '?')
                orig_mode = rec.get('MODE', '?')
                result[i] = (f'Same call/date, time +/-{diff}min, '
                             f'band/mode differs ({orig_band}/{orig_mode})')
                break

    return result


def normalize_record(rec: dict) -> dict:
    """Normalize a record's CALL, BAND, and MODE fields in place."""
    if 'CALL' in rec:
        rec['CALL'] = rec['CALL'].upper().strip()
    if 'BAND' in rec:
        rec['BAND'] = norm_band(rec['BAND'])
    if 'MODE' in rec:
        rec['MODE'] = norm_mode(rec['MODE'])
    return rec


def infer_missing_bands(records: list[dict], max_gap_mins: int = 30) -> int:
    """Fill in blank BAND fields by looking at surrounding QSOs.

    Sorts records by date+time, then for each record with a blank band,
    checks the nearest QSOs before and after. If both neighbors are on
    the same band within max_gap_mins, that band is assigned. If only
    one neighbor is within range, uses that band.

    Modifies records in place. Returns count of bands filled.
    """
    # Sort by date + time
    def _sort_key(r: dict) -> str:
        return r.get('QSO_DATE', '') + (r.get('TIME_ON', '') + '000000')[:6]

    sorted_recs = sorted(records, key=_sort_key)

    # Build index of records with known bands
    filled = 0
    for i, rec in enumerate(sorted_recs):
        band = norm_band(rec.get('BAND', ''))
        if band:
            continue  # already has a band

        t = time_to_mins(rec.get('TIME_ON', ''))
        d = rec.get('QSO_DATE', '')

        # Look backward for nearest record with a band
        prev_band = ''
        prev_gap = 999999
        for j in range(i - 1, max(i - 20, -1), -1):
            pb = norm_band(sorted_recs[j].get('BAND', ''))
            if not pb:
                continue
            pd = sorted_recs[j].get('QSO_DATE', '')
            pt = time_to_mins(sorted_recs[j].get('TIME_ON', ''))
            if pd == d:
                gap = abs(t - pt)
            elif date_to_ord(d) is not None and date_to_ord(pd) is not None:
                day_diff = abs(date_to_ord(d) - date_to_ord(pd))
                if day_diff > 1:
                    break
                gap = abs(t - pt) + day_diff * 1440
            else:
                break
            if gap <= max_gap_mins:
                prev_band = pb
                prev_gap = gap
            break

        # Look forward for nearest record with a band
        next_band = ''
        next_gap = 999999
        for j in range(i + 1, min(i + 20, len(sorted_recs))):
            nb = norm_band(sorted_recs[j].get('BAND', ''))
            if not nb:
                continue
            nd = sorted_recs[j].get('QSO_DATE', '')
            nt = time_to_mins(sorted_recs[j].get('TIME_ON', ''))
            if nd == d:
                gap = abs(t - nt)
            elif date_to_ord(d) is not None and date_to_ord(nd) is not None:
                day_diff = abs(date_to_ord(d) - date_to_ord(nd))
                if day_diff > 1:
                    break
                gap = abs(t - nt) + day_diff * 1440
            else:
                break
            if gap <= max_gap_mins:
                next_band = nb
                next_gap = gap
            break

        # Decide
        inferred = ''
        if prev_band and next_band:
            if prev_band == next_band:
                inferred = prev_band  # both neighbors agree
            else:
                # Different bands — use the closer one
                inferred = prev_band if prev_gap <= next_gap else next_band
        elif prev_band:
            inferred = prev_band
        elif next_band:
            inferred = next_band

        if inferred:
            rec['BAND'] = inferred
            rec['_BAND_INFERRED'] = 'yes'
            filled += 1

    return filled
