#!/usr/bin/env python3
"""
Ham Radio Log Sync
==================
Compares contacts across HRD Local, QRZ.com, eQSL.cc, HRDlog.net, and GridTracker2.
Finds missing entries in each service and pushes them to fill the gaps.

Requirements:
  pip install requests

Usage:
  python log_sync.py
"""

from __future__ import annotations

import json
import os
import re
import threading
from datetime import datetime
from tkinter import filedialog, messagebox
from typing import Optional
import tkinter as tk
from tkinter import ttk

import requests

# =============================================================================
#  CONFIG
# =============================================================================

# Config file lives in the same directory as this script (or the packaged .exe)
_APP_DIR    = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(_APP_DIR, 'log_sync_config.json')
APP_TITLE   = "Ham Radio Log Sync"


# =============================================================================
#  ADIF UTILITIES — delegated to log_utils.py (shared, no GUI deps)
# =============================================================================

from log_utils import (
    parse_adif_records  as _parse_adif_records,
    build_adif          as _build_adif,
    norm_band           as _norm_band,
    norm_mode           as _norm_mode,
    qso_key             as _qso_key,
    keys_of             as _keys_of,
    find_missing        as _find_missing,
    is_round_time       as _is_round_time,
    dedupe_prefer_exact_time as _dedupe_prefer_exact_time,
    time_to_mins        as _time_to_mins,
    date_to_ord         as _date_to_ord,
    near_dupe_indices   as _near_dupe_indices,
    BAND_ALIASES        as _BAND_ALIASES,
    MODE_ALIASES        as _MODE_ALIASES,
)


# =============================================================================
#  CONFIG PERSISTENCE
# =============================================================================

_DEFAULT_ADVANCED: dict = {
    # Dedup / matching
    'dedup_window_mins':      1,    # Two contacts within this many minutes are treated as the same QSO
    'near_dupe_warn_mins':    15,   # Flag as near-dupe warning in review dialog within this many minutes
    # QRZ.com
    'qrz_fetch_page_size':    250,  # Records per API fetch page (max 250)
    'qrz_push_batch_size':    100,  # Records per API push batch
    # HRDlog.net
    'hrdlog_push_batch_size': 50,   # Records per API push batch
    # eQSL.cc
    'eqsl_rcvd_since':        '01011990',  # Fetch eQSL inbox cards received since (MMDDYYYY)
    # Network timeouts (seconds)
    'timeout_short':          30,   # Used for fetch/read requests
    'timeout_long':           60,   # Used for push/upload requests
}

def _detect_gt2_path() -> str:
    """
    Auto-detect the GridTracker2 backup ADIF.
    Looks in %APPDATA%\\GridTracker2\\Backup Logs\\ for the first .adif file.
    Returns the full path if found, else ''.
    """
    folder = os.path.join(os.environ.get('APPDATA', ''), 'GridTracker2', 'Backup Logs')
    if not os.path.isdir(folder):
        return ''
    for fname in os.listdir(folder):
        if fname.lower().endswith(('.adif', '.adi')):
            return os.path.join(folder, fname)
    return ''

_DEFAULT_CONFIG: dict = {
    'hrd_path':     '',
    'qrz_key':      '',
    'qrz_path':     '',
    'eqsl_user':    '',
    'eqsl_pass':    '',
    'eqsl_path':    '',
    'hrdlog_call':  '',
    'hrdlog_code':  '',
    'hrdlog_path':  '',
    'gt2_path':     '',
    # LoTW (used by Log Scanner)
    'lotw_user':    '',
    'lotw_pass':    '',
    # ClubLog (used by Log Scanner)
    'clublog_call':  '',
    'clublog_email': '',
    'clublog_pass':  '',
    'clublog_api':   '',
    'advanced':     dict(_DEFAULT_ADVANCED),
}


def load_config() -> dict:
    try:
        with open(CONFIG_FILE, encoding='utf-8') as f:
            data = json.load(f)
        cfg = dict(_DEFAULT_CONFIG)
        cfg.update(data)
        # Deep-merge advanced section so new keys added in _DEFAULT_ADVANCED
        # are always present even if the saved config pre-dates them
        adv = dict(_DEFAULT_ADVANCED)
        adv.update(cfg.get('advanced') or {})
        cfg['advanced'] = adv
    except FileNotFoundError:
        cfg = dict(_DEFAULT_CONFIG)
    except Exception as e:
        print(f'[CONFIG] Load error: {e}')
        cfg = dict(_DEFAULT_CONFIG)
    # Auto-detect GT2 backup path if not yet configured
    if not cfg.get('gt2_path'):
        detected = _detect_gt2_path()
        if detected:
            cfg['gt2_path'] = detected
    return cfg


def save_config(cfg: dict) -> None:
    try:
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(cfg, f, indent=2)
    except Exception as e:
        print(f'[CONFIG] Save error: {e}')


# =============================================================================
#  CONNECTORS
# =============================================================================

class LocalHRDConnector:
    """
    Reads from ExportAll.adi.
    Push = generate a dated ADIF file for manual import via HRD File -> Import.
    """
    name = 'HRD Local'

    def __init__(self, path: str):
        self.path = path.strip()

    def fetch(self, log=None) -> list:
        if not self.path or not os.path.exists(self.path):
            raise FileNotFoundError(f'HRD ADIF not found: {self.path!r}. Set path in Settings.')
        with open(self.path, encoding='utf-8-sig', errors='replace') as f:
            content = f.read()
        recs = _parse_adif_records(content)
        if log:
            log(f'HRD Local: loaded {len(recs):,} records from {self.path}')
        return recs

    def push(self, records: list, log) -> str:
        """Write records to a timestamped ADIF file for manual HRD import."""
        if not records:
            log('HRD Local: nothing to push.')
            return ''
        base_dir = os.path.dirname(self.path) if self.path else os.getcwd()
        stamp    = datetime.now().strftime('%Y%m%d_%H%M%S')
        out_path = os.path.join(base_dir, f'hrd_import_{stamp}.adi')
        with open(out_path, 'w', encoding='utf-8') as f:
            f.write(_build_adif(records))
        log(f'HRD Local: wrote {len(records):,} records to {out_path}')
        log('HRD Local: File -> Import -> ADIF in Ham Radio Deluxe Logbook, then select the file above.')
        return out_path


class QRZConnector:
    """
    QRZ.com logbook sync.
    Fetch: uses local ADIF file if qrz_path is set, otherwise falls back to API.
    Push: always uses the API key.
    """

    name    = 'QRZ.com'
    API_URL = 'https://logbook.qrz.com/api'

    def __init__(self, api_key: str, adif_path: str = '',
                 fetch_page_size: int = 250, push_batch_size: int = 100,
                 timeout_short: int = 30, timeout_long: int = 60):
        self.api_key        = api_key.strip()
        self.adif_path      = adif_path.strip()
        self.fetch_page_size = int(fetch_page_size)
        self.push_batch_size = int(push_batch_size)
        self.timeout_short  = int(timeout_short)
        self.timeout_long   = int(timeout_long)

    @staticmethod
    def _parse_response(text: str) -> dict:
        """
        Parse QRZ API response (application/x-www-form-urlencoded style).
        Splits only on '&' that is immediately followed by an ALL-CAPS key name,
        so ADIF content embedded in the value is never fragmented.
        """
        result: dict = {}
        parts = re.split(r'&(?=[A-Z]+=)', text)
        for part in parts:
            if '=' in part:
                key, _, val = part.partition('=')
                key = key.strip()
                if re.fullmatch(r'[A-Z]+', key):
                    result[key] = val.strip()
        return result

    def _post(self, params: dict) -> dict:
        p = dict(params)
        p['KEY'] = self.api_key
        resp = requests.post(self.API_URL, data=p, timeout=self.timeout_short)
        resp.raise_for_status()
        return self._parse_response(resp.text)

    def fetch(self, log=None) -> list:
        # Prefer local file if provided
        if self.adif_path:
            if not os.path.exists(self.adif_path):
                raise FileNotFoundError(f'QRZ ADIF not found: {self.adif_path}')
            with open(self.adif_path, encoding='utf-8-sig', errors='replace') as f:
                content = f.read()
            recs = _parse_adif_records(content)
            if log:
                log(f'QRZ.com: loaded {len(recs):,} records from {self.adif_path}')
            return recs

        # Fall back to API
        if not self.api_key:
            raise ValueError('QRZ: no ADIF file or API key configured. Add one in Settings.')
        all_records: list = []
        after_id   = 0
        page       = 0
        while True:
            page  += 1
            params = {'ACTION': 'FETCH', 'MAX': str(self.fetch_page_size)}
            if after_id:
                params['OPTION'] = f'AFTERLOGID:{after_id}'
            result = self._post(params)
            if log:
                preview = {k: (v[:80] if k == 'ADIF' else v) for k, v in result.items()}
                log(f'QRZ.com: page {page} response: {preview}')
            if result.get('RESULT') == 'FAIL':
                raise RuntimeError(f"QRZ FETCH failed: {result.get('REASON', result)}")
            adif_text = result.get('ADIF', '')
            recs      = _parse_adif_records(adif_text)
            if log:
                log(f'QRZ.com: page {page} -> {len(recs)} records')
            all_records.extend(recs)
            if len(recs) < self.fetch_page_size:
                break
            logids   = [int(r['LOGID']) for r in recs if r.get('LOGID', '').isdigit()]
            after_id = max(logids) if logids else 0
            if not logids:
                break
        return all_records

    def push(self, records: list, log) -> None:
        if not records:
            log('QRZ.com: nothing to push.')
            return
        if not self.api_key:
            raise ValueError('QRZ API key not configured.')
        BATCH   = self.push_batch_size
        pushed  = 0
        skipped = 0
        total   = len(records)

        for i in range(0, total, BATCH):
            batch  = records[i:i + BATCH]
            adif   = _build_adif(batch)
            result = self._post({'ACTION': 'INSERT', 'ADIF': adif})
            if result.get('RESULT') != 'FAIL':
                pushed += len(batch)
                log(f'QRZ.com: pushed {pushed:,}/{total:,}')
                continue
            reason = result.get('REASON', '')
            if 'duplicate' not in reason.lower():
                raise RuntimeError(f'QRZ INSERT failed: {reason or result}')
            # Batch contains at least one problem record — retry one-by-one
            log(f'QRZ.com: batch {i//BATCH + 1} has rejected record(s), retrying individually...')
            for rec in batch:
                r2 = self._post({'ACTION': 'INSERT', 'ADIF': _build_adif([rec])})
                if r2.get('RESULT') != 'FAIL':
                    pushed += 1
                else:
                    reason2 = r2.get('REASON', '')
                    call    = rec.get('CALL', '?')
                    date    = rec.get('QSO_DATE', '?')
                    if 'duplicate' in reason2.lower():
                        skipped += 1
                    else:
                        skipped += 1
                        log(f'QRZ.com: skipped {call} {date} — {reason2}')

        msg = f'QRZ.com: pushed {pushed:,}'
        if skipped:
            msg += f', {skipped:,} skipped (duplicates or rejected — see log above)'
        log(msg)


class EQSLConnector:
    """
    eQSL.cc sync.
    Fetch: uses local ADIF file if eqsl_path is set, otherwise tries the inbox API
           (inbox API only returns received/confirmed cards, not your full sent log).
    Push: always uses the ImportADIF.cfm API with username + password.
    """
    name         = 'eQSL.cc'
    UPLOAD_URL   = 'https://www.eqsl.cc/qslcard/ImportADIF.cfm'
    DOWNLOAD_URL = 'https://www.eqsl.cc/qslcard/DownloadInbox.cfm'

    def __init__(self, user: str, password: str, adif_path: str = '',
                 rcvd_since: str = '01011990',
                 timeout_short: int = 30, timeout_long: int = 60):
        self.user          = user.strip()
        self.password      = password.strip()
        self.adif_path     = adif_path.strip()
        self.rcvd_since    = rcvd_since.strip() or '01011990'
        self.timeout_short = int(timeout_short)
        self.timeout_long  = int(timeout_long)

    def fetch(self, log=None) -> list:
        # Prefer local file if provided (full log export from eQSL.cc website)
        if self.adif_path:
            if not os.path.exists(self.adif_path):
                raise FileNotFoundError(f'eQSL ADIF not found: {self.adif_path}')
            with open(self.adif_path, encoding='utf-8-sig', errors='replace') as f:
                content = f.read()
            recs = _parse_adif_records(content)
            if log:
                log(f'eQSL.cc: loaded {len(recs):,} records from {self.adif_path}')
            return recs

        # Fall back to inbox API (received/confirmed cards only)
        if not self.user or not self.password:
            if log:
                log('eQSL.cc: no ADIF file or credentials configured — skipping.')
            return []
        if log:
            log('eQSL.cc: no ADIF file set; fetching inbox (received cards only)...')
        resp = requests.get(self.DOWNLOAD_URL, params={
            'UserName':  self.user,
            'Password':  self.password,
            'RcvdSince': self.rcvd_since,
        }, timeout=self.timeout_short)
        resp.raise_for_status()
        if log:
            log(f'eQSL.cc: response preview: {resp.text[:200]}')
        if '<html' in resp.text.lower() and '<eor>' not in resp.text.lower():
            raise RuntimeError(f'eQSL returned HTML (auth failed?): {resp.text[:300]}')
        recs = _parse_adif_records(resp.text)
        if log:
            log(f'eQSL.cc: fetched {len(recs):,} received cards from inbox')
        return recs

    def push(self, records: list, log) -> None:
        if not records:
            log('eQSL.cc: nothing to push.')
            return
        if not self.user or not self.password:
            raise ValueError('eQSL username/password not configured.')
        adif = _build_adif(records)
        # eQSL ImportADIF.cfm field names (as exposed in their error messages)
        resp = requests.post(
            self.UPLOAD_URL,
            data={
                'eQSL_User': self.user,
                'eQSL_Pswd': self.password,
                'ADIFData':  adif,
            },
            timeout=self.timeout_long,
        )
        resp.raise_for_status()
        text = resp.text
        log(f'eQSL.cc: response: {text[:300]}')
        if 'error' in text.lower():
            log(f'eQSL.cc: upload may have issues — check response above.')
        else:
            log(f'eQSL.cc: upload complete — {len(records):,} records sent.')


class HRDlogConnector:
    """
    Push to HRDlog.net via XML API.
    Fetch loads a manually exported ADIF file from hrdlog.net.
    """
    name    = 'HRDlog.net'
    API_URL = 'http://www.hrdlog.net/api/newlogbook.aspx'

    def __init__(self, callsign: str, upload_code: str, adif_path: str = '',
                 push_batch_size: int = 50, timeout_long: int = 60):
        self.callsign       = callsign.strip().upper()
        self.upload_code    = upload_code.strip()
        self.adif_path      = adif_path.strip()
        self.push_batch_size = int(push_batch_size)
        self.timeout_long   = int(timeout_long)

    def fetch(self, log=None) -> list:
        """Load manually exported ADIF from hrdlog.net."""
        if not self.adif_path:
            if log:
                log('HRDlog.net: no manual ADIF path set — skipping. '
                    'Export your log from hrdlog.net and set the path in Settings.')
            return []
        if not os.path.exists(self.adif_path):
            raise FileNotFoundError(f'HRDlog ADIF not found: {self.adif_path}')
        with open(self.adif_path, encoding='utf-8-sig', errors='replace') as f:
            content = f.read()
        recs = _parse_adif_records(content)
        if log:
            log(f'HRDlog.net: loaded {len(recs):,} records from {self.adif_path}')
        return recs

    def push(self, records: list, log) -> None:
        if not records:
            log('HRDlog.net: nothing to push.')
            return
        if not self.callsign or not self.upload_code:
            raise ValueError('HRDlog callsign/upload code not configured.')
        BATCH  = self.push_batch_size
        pushed = 0
        for i in range(0, len(records), BATCH):
            batch = records[i:i + BATCH]
            adif  = _build_adif(batch)
            resp  = requests.post(
                self.API_URL,
                params={'user': self.callsign, 'api': self.upload_code},
                data=adif.encode('utf-8'),
                headers={'Content-Type': 'text/plain'},
                timeout=self.timeout_long,
            )
            resp.raise_for_status()
            pushed += len(batch)
            log(f'HRDlog.net: pushed {pushed:,}/{len(records):,}. Response: {resp.text[:120]}')


class GridTracker2Connector:
    """
    Read-only connector for GridTracker2's automatic ADIF backup log.

    GridTracker2 writes a live ADIF backup to:
      %APPDATA%\\GridTracker2\\Backup Logs\\<CALL>_<GRID>.adif

    This connector reads that file so GT2's contacts are included in
    missing-contact calculations for QRZ, eQSL, and HRDlog.
    Push is not supported — GridTracker2 has no import API.
    """
    name = 'GridTracker2'

    def __init__(self, path: str):
        self.path = path.strip()

    def fetch(self, log=None) -> list:
        if not self.path:
            if log:
                log('GridTracker2: no ADIF path configured — skipping. '
                    'Set path in Settings to the Backup Logs file in the '
                    'GridTracker2 AppData folder.')
            return []
        if not os.path.exists(self.path):
            raise FileNotFoundError(
                f'GridTracker2 ADIF not found: {self.path!r}. '
                'Update the path in Settings.')
        with open(self.path, encoding='utf-8-sig', errors='replace') as f:
            content = f.read()
        recs = _parse_adif_records(content)
        if log:
            log(f'GridTracker2: loaded {len(recs):,} records from '
                f'{os.path.basename(self.path)}')
        return recs

    def push(self, records: list, log) -> str:
        """
        Write missing records to a dated ADIF file for manual import into
        GridTracker2 (File -> Import ADIF, if supported, or append manually).
        Returns the output file path.
        """
        if not records:
            log('GridTracker2: nothing to push.')
            return ''
        base_dir = os.path.dirname(self.path) if self.path else os.getcwd()
        stamp    = datetime.now().strftime('%Y%m%d_%H%M%S')
        out_path = os.path.join(base_dir, f'gt2_import_{stamp}.adi')
        with open(out_path, 'w', encoding='utf-8') as f:
            f.write(_build_adif(records))
        log(f'GridTracker2: wrote {len(records):,} records to {out_path}')
        log('GridTracker2: import this file via GridTracker2 File -> Import ADIF '
            '(or add it to the Backup Logs folder while GT2 is closed).')
        return out_path


# =============================================================================
#  SYNC ENGINE
# =============================================================================

SOURCES     = ['hrd', 'qrz', 'eqsl', 'hrdlog', 'gt2']
SRC_LABELS  = {'hrd': 'HRD Local', 'qrz': 'QRZ.com',
               'eqsl': 'eQSL.cc', 'hrdlog': 'HRDlog.net',
               'gt2': 'GridTracker2'}


class SyncEngine:
    """Holds fetched records and computes which records are missing from each source."""

    def __init__(self):
        self.records: dict[str, list] = {s: [] for s in SOURCES}
        self.keys:    dict[str, set]  = {s: set() for s in SOURCES}
        self.fetched: dict[str, bool] = {s: False for s in SOURCES}

    def store(self, source: str, records: list) -> None:
        self.records[source] = records
        self.keys[source]    = _keys_of(records)
        self.fetched[source] = True

    def missing_from_target(self, target: str, window: int = 1) -> list:
        """
        Return all unique records that exist in any fetched source
        but are absent from target.  Each record is a copy tagged with
        '_src' (source label) for display purposes; '_src' is stripped
        before any ADIF is built.
        """
        if not self.fetched[target]:
            return []
        seen:   set  = set()
        result: list = []
        for src in SOURCES:
            if src == target or not self.fetched[src]:
                continue
            for rec in _find_missing(self.records[src], self.records[target], window):
                k = _qso_key(rec)
                if k not in seen:
                    seen.add(k)
                    tagged = dict(rec)
                    tagged['_src'] = SRC_LABELS[src]
                    result.append(tagged)
        return _dedupe_prefer_exact_time(result, window)

    def missing_summary_for_row(self, source: str, window: int = 1) -> str:
        """
        For a given source row, describe which targets are missing
        records that exist in this source.
        """
        if not self.fetched[source]:
            return '-'
        parts = []
        for tgt in SOURCES:
            if tgt == source or not self.fetched[tgt]:
                continue
            n = len(_find_missing(self.records[source], self.records[tgt], window))
            if n:
                parts.append(f'{SRC_LABELS[tgt]}:{n:,}')
        return '  '.join(parts) if parts else 'none'


# Near-dupe detection functions now imported from log_utils above.

# =============================================================================
#  REVIEW DIALOG
# =============================================================================

class ReviewDialog(tk.Toplevel):
    """
    Modal pre-push review window.
    Shows every record that would be pushed to a target service.
    Rows highlighted in gold are near-duplicates (same call/date/band/mode,
    TIME_ON within ±5 minutes of an existing record in the target) — they may
    already exist under a slightly different timestamp.
    The user can deselect any rows before clicking Push Selected.
    """

    _BG    = '#1e1e1e'
    _FG    = '#e0e0e0'
    _SEL   = '#0078d7'
    _WARN  = '#5a4400'
    _WFORE = '#ffd966'

    def __init__(self, parent, records: list, near_idx: dict,
                 target_label: str, on_push):
        super().__init__(parent)
        self.title(f'Review before pushing to {target_label}')
        self.geometry('980x580')
        self.minsize(720, 400)
        self.configure(bg=self._BG)
        self.grab_set()   # modal

        self._records      = records
        self._near_idx     = near_idx
        self._on_push      = on_push
        self._target_label = target_label

        self._build_ui()
        self._populate()

    # ------------------------------------------------------------------
    def _build_ui(self) -> None:
        # ---- info bar ----
        info = tk.Frame(self, bg=self._BG, pady=6, padx=10)
        info.pack(fill='x')
        tk.Label(info,
                 text=f'{len(self._records):,} records would be pushed to {self._target_label}.',
                 bg=self._BG, fg=self._FG,
                 font=('Segoe UI', 9)).pack(side='left')
        if self._near_idx:
            tk.Label(info,
                     text=f'  {len(self._near_idx):,} highlighted in gold are possible near-duplicates '
                          f'(see Near-Dupe Warning column) — deselect before pushing if already logged.',
                     bg=self._BG, fg=self._WFORE,
                     font=('Segoe UI', 9)).pack(side='left')

        # ---- treeview ----
        tv_frame = tk.Frame(self, bg=self._BG)
        tv_frame.pack(fill='both', expand=True, padx=10, pady=(0, 4))

        style = ttk.Style()
        style.configure('Rev.Treeview',
                        background='#2a2a2a', foreground=self._FG,
                        fieldbackground='#2a2a2a', rowheight=22)
        style.configure('Rev.Treeview.Heading',
                        background='#252526', foreground=self._FG, relief='flat')
        style.map('Rev.Treeview', background=[('selected', self._SEL)])

        cols = ('sel', 'call', 'date', 'time', 'band', 'mode', 'src', 'note')
        self._tv = ttk.Treeview(tv_frame, columns=cols, show='headings',
                                selectmode='extended', style='Rev.Treeview')

        for col, hdr, w, anchor in [
            ('sel',  '',                  24,  'center'),
            ('call', 'Callsign',         110,  'w'),
            ('date', 'Date',              90,  'w'),
            ('time', 'Time (UTC)',         70,  'w'),
            ('band', 'Band',              65,  'w'),
            ('mode', 'Mode',              70,  'w'),
            ('src',  'Source',           110,  'w'),
            ('note', 'Near-Dupe Warning', 330,  'w'),
        ]:
            self._tv.heading(col, text=hdr, anchor=anchor)
            self._tv.column(col, width=w, anchor=anchor, stretch=(col == 'note'))

        self._tv.tag_configure('warn', background=self._WARN, foreground=self._WFORE)

        vsb = ttk.Scrollbar(tv_frame, orient='vertical', command=self._tv.yview)
        self._tv.configure(yscrollcommand=vsb.set)
        self._tv.pack(side='left', fill='both', expand=True)
        vsb.pack(side='right', fill='y')

        self._tv.bind('<<TreeviewSelect>>', lambda _e: self._sync_checkmarks())

        # ---- button bar ----
        bar = tk.Frame(self, bg=self._BG, pady=8, padx=10)
        bar.pack(fill='x')

        self._push_btn = tk.Button(
            bar, text='Push Selected  (0)',
            bg='#0078d7', fg='white', font=('Segoe UI', 9, 'bold'),
            relief='flat', padx=10, pady=5,
            command=self._do_push)
        self._push_btn.pack(side='right', padx=(6, 0))

        tk.Button(bar, text='Cancel',
                  bg='#3c3c3c', fg=self._FG, relief='flat', padx=10, pady=5,
                  command=self.destroy).pack(side='right')

        tk.Button(bar, text='Select All',
                  bg='#3c3c3c', fg=self._FG, relief='flat', padx=8, pady=5,
                  command=self._select_all).pack(side='left')
        tk.Button(bar, text='Deselect Near-Dupes',
                  bg='#3c3c3c', fg=self._FG, relief='flat', padx=8, pady=5,
                  command=self._deselect_near).pack(side='left', padx=(6, 0))
        tk.Button(bar, text='Deselect All',
                  bg='#3c3c3c', fg=self._FG, relief='flat', padx=8, pady=5,
                  command=self._deselect_all).pack(side='left', padx=(6, 0))

    # ------------------------------------------------------------------
    def _populate(self) -> None:
        for i, rec in enumerate(self._records):
            note  = self._near_idx.get(i, '')
            is_nd = bool(note)
            self._tv.insert('', 'end', iid=str(i),
                            values=(
                                '',
                                rec.get('CALL',     ''),
                                rec.get('QSO_DATE', ''),
                                rec.get('TIME_ON',  '')[:4],
                                rec.get('BAND',     ''),
                                rec.get('MODE',     ''),
                                rec.get('_src',     ''),
                                note,
                            ),
                            tags=('warn',) if is_nd else ())
        self._select_all()

    # ------------------------------------------------------------------
    def _select_all(self) -> None:
        self._tv.selection_set(self._tv.get_children())
        self._sync_checkmarks()

    def _deselect_all(self) -> None:
        self._tv.selection_set(())
        self._sync_checkmarks()

    def _deselect_near(self) -> None:
        near_iids = {str(i) for i in self._near_idx}
        keep = [iid for iid in self._tv.selection() if iid not in near_iids]
        self._tv.selection_set(keep)
        self._sync_checkmarks()

    def _sync_checkmarks(self) -> None:
        sel = set(self._tv.selection())
        for iid in self._tv.get_children():
            cur = self._tv.item(iid, 'values')
            mark = '✓' if iid in sel else ''
            self._tv.item(iid, values=(mark,) + cur[1:])
        n = len(sel)
        self._push_btn.configure(text=f'Push Selected  ({n:,})')

    # ------------------------------------------------------------------
    def _do_push(self) -> None:
        indices = [int(iid) for iid in self._tv.selection()]
        records = [self._records[i] for i in sorted(indices)]
        if not records:
            return
        self.destroy()
        self._on_push(records)


# =============================================================================
#  GUI
# =============================================================================

class LogSyncApp(tk.Tk):

    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry('860x700')
        self.minsize(700, 500)
        self.resizable(True, True)
        self.configure(bg='#1e1e1e')

        self.cfg    = load_config()
        self.engine = SyncEngine()
        self._busy  = False

        self._build_ui()
        self._load_settings_to_ui()

    # -------------------------------------------------------------------------
    # UI construction
    # -------------------------------------------------------------------------

    def _build_ui(self) -> None:
        style = ttk.Style(self)
        style.theme_use('clam')

        BG   = '#1e1e1e'
        FG   = '#e0e0e0'
        SEL  = '#0078d7'
        ENTR = '#2d2d2d'
        BTN  = '#3c3c3c'
        HEAD = '#252526'

        style.configure('.',              background=BG,   foreground=FG,
                        font=('Segoe UI', 9))
        style.configure('TFrame',         background=BG)
        style.configure('TLabel',         background=BG,   foreground=FG)
        style.configure('TEntry',         fieldbackground=ENTR, foreground=FG,
                        insertcolor=FG)
        style.configure('TButton',        background=BTN,  foreground=FG, padding=4)
        style.map('TButton',              background=[('active', SEL)])
        style.configure('TNotebook',      background=BG,   tabmargins=[2, 2, 2, 0])
        style.configure('TNotebook.Tab',  background=BTN,  foreground=FG,
                        padding=[12, 5])
        style.map('TNotebook.Tab',        background=[('selected', SEL)])
        style.configure('Treeview',       background=ENTR, foreground=FG,
                        fieldbackground=ENTR, rowheight=24)
        style.configure('Treeview.Heading', background=HEAD, foreground=FG,
                        relief='flat')
        style.map('Treeview',             background=[('selected', SEL)])
        style.configure('TSeparator',     background='#444444')

        nb = ttk.Notebook(self)
        nb.pack(fill='both', expand=True, padx=8, pady=8)

        self._tab_settings = ttk.Frame(nb)
        self._tab_sync     = ttk.Frame(nb)
        nb.add(self._tab_settings, text='  Settings  ')
        nb.add(self._tab_sync,     text='  Sync  ')

        self._build_settings_tab()
        self._build_sync_tab()

    # -- helpers --

    def _make_row(self, parent, row: int, label: str, attr: str,
                  show: str = '', browse: bool = False) -> ttk.Entry:
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky='w',
                                            pady=3, padx=(0, 10))
        var = tk.StringVar()
        setattr(self, attr, var)
        ent = ttk.Entry(parent, textvariable=var, width=54, show=show)
        ent.grid(row=row, column=1, sticky='ew', pady=3)
        if browse:
            ttk.Button(parent, text='...', width=3,
                       command=lambda v=var: self._browse(v)
                       ).grid(row=row, column=2, padx=(5, 0), pady=3)
        return ent

    def _browse(self, var: tk.StringVar) -> None:
        path = filedialog.askopenfilename(
            filetypes=[('ADIF files', '*.adi *.adif'), ('All files', '*.*')]
        )
        if path:
            var.set(path)

    def _section(self, parent, row: int, text: str) -> None:
        ttk.Label(parent, text=text,
                  font=('Segoe UI', 9, 'bold')).grid(
            row=row, column=0, columnspan=3, sticky='w', pady=(6, 1))

    def _sep(self, parent, row: int) -> None:
        ttk.Separator(parent, orient='horizontal').grid(
            row=row, column=0, columnspan=3, sticky='ew', pady=6)

    # -- settings tab --

    def _build_settings_tab(self) -> None:
        p = ttk.Frame(self._tab_settings, padding=20)
        p.pack(fill='both', expand=True)
        p.columnconfigure(1, weight=1)

        r = 0
        self._section(p, r, 'HRD Local'); r += 1
        self._make_row(p, r, 'ExportAll.adi path:', '_v_hrd_path', browse=True); r += 1
        self._sep(p, r); r += 1

        self._section(p, r, 'QRZ.com'); r += 1
        self._make_row(p, r, 'API Key (for push):', '_v_qrz_key'); r += 1
        self._make_row(p, r, 'Manual ADIF export:', '_v_qrz_path', browse=True); r += 1
        ttk.Label(p, text='QRZ: download your log from qrz.com Logbook -> Export as ADIF, then browse above. '
                          'If set, this is used for fetch (API key is still needed to push).',
                  foreground='#888888', wraplength=520).grid(
            row=r, column=0, columnspan=3, sticky='w', pady=(2, 0)); r += 1
        self._sep(p, r); r += 1

        self._section(p, r, 'eQSL.cc'); r += 1
        self._make_row(p, r, 'Username:', '_v_eqsl_user'); r += 1
        self._make_row(p, r, 'Password:', '_v_eqsl_pass', show='*'); r += 1
        self._make_row(p, r, 'Manual ADIF export:', '_v_eqsl_path', browse=True); r += 1
        ttk.Label(p, text='eQSL: log in at eqsl.cc -> My Log -> Download/Export ADIF, then browse above. '
                          'Username + password are used for push.',
                  foreground='#888888', wraplength=520).grid(
            row=r, column=0, columnspan=3, sticky='w', pady=(2, 0)); r += 1
        self._sep(p, r); r += 1

        self._section(p, r, 'HRDlog.net'); r += 1
        self._make_row(p, r, 'Callsign:', '_v_hrdlog_call'); r += 1
        self._make_row(p, r, 'Upload Code:', '_v_hrdlog_code'); r += 1
        self._make_row(p, r, 'Manual ADIF export:', '_v_hrdlog_path', browse=True); r += 1
        ttk.Label(p, text='HRDlog: log in at hrdlog.net -> Tools -> Export Log as ADIF, then browse above.',
                  foreground='#888888', wraplength=520).grid(
            row=r, column=0, columnspan=3, sticky='w', pady=(2, 0)); r += 1
        self._sep(p, r); r += 1

        self._section(p, r, 'GridTracker2  (read-only — fetch only, no push)'); r += 1
        self._make_row(p, r, 'Backup ADIF path:', '_v_gt2_path', browse=True); r += 1
        ttk.Label(p,
                  text=r'GridTracker2 auto-saves a live ADIF log to: '
                       r'%APPDATA%\GridTracker2\Backup Logs\<CALL>_<GRID>.adif  '
                       r'Browse to that file above. Contacts from GT2 will be included '
                       r'when computing what is missing from QRZ / eQSL / HRDlog.',
                  foreground='#888888', wraplength=520).grid(
            row=r, column=0, columnspan=3, sticky='w', pady=(2, 0)); r += 1

        ttk.Button(p, text='Save Settings',
                   command=self._save_settings).grid(
            row=r, column=2, sticky='e', pady=(14, 0))

    # -- sync tab --

    def _build_sync_tab(self) -> None:
        p = ttk.Frame(self._tab_sync, padding=12)
        p.pack(fill='both', expand=True)
        p.columnconfigure(0, weight=1)
        p.rowconfigure(4, weight=1)   # log area stretches

        # --- top bar ---
        top = ttk.Frame(p)
        top.grid(row=0, column=0, columnspan=2, sticky='ew', pady=(0, 10))
        ttk.Button(top, text='Fetch All Logs',
                   command=self._fetch_all).pack(side='left')
        self._v_status = tk.StringVar(value='Status: idle')
        ttk.Label(top, textvariable=self._v_status).pack(side='left', padx=16)

        # --- summary treeview ---
        cols = ('source', 'contacts', 'fetched', 'missing')
        self._tree = ttk.Treeview(p, columns=cols, show='headings', height=6)
        self._tree.heading('source',   text='Source',              anchor='w')
        self._tree.heading('contacts', text='Contacts',            anchor='center')
        self._tree.heading('fetched',  text='Status',              anchor='center')
        self._tree.heading('missing',  text='This source missing from...', anchor='w')
        self._tree.column('source',   width=110, anchor='w')
        self._tree.column('contacts', width=90,  anchor='center')
        self._tree.column('fetched',  width=70,  anchor='center')
        self._tree.column('missing',  width=440, anchor='w')
        self._tree.grid(row=1, column=0, sticky='ew', pady=(0, 10))

        for src in SOURCES:
            self._tree.insert('', 'end', iid=src,
                              values=(SRC_LABELS[src], '-', '-', '-'))

        # --- push buttons ---
        btn_f = ttk.Frame(p)
        btn_f.grid(row=2, column=0, sticky='ew', pady=(0, 10))

        self._btns: dict[str, ttk.Button] = {}

        self._btns['qrz']    = ttk.Button(btn_f, text='Push missing -> QRZ',
                                           command=lambda: self._push_to('qrz'),
                                           state='disabled')
        self._btns['eqsl']   = ttk.Button(btn_f, text='Push missing -> eQSL',
                                           command=lambda: self._push_to('eqsl'),
                                           state='disabled')
        self._btns['hrdlog'] = ttk.Button(btn_f, text='Push missing -> HRDlog',
                                           command=lambda: self._push_to('hrdlog'),
                                           state='disabled')
        self._btns['hrd']    = ttk.Button(btn_f, text='Generate ADIF for HRD import',
                                           command=self._gen_hrd_adif,
                                           state='disabled')
        self._btns['gt2']    = ttk.Button(btn_f, text='Generate ADIF for GT2 import',
                                           command=self._gen_gt2_adif,
                                           state='disabled')

        for btn in self._btns.values():
            btn.pack(side='left', padx=(0, 6))

        # --- separator ---
        ttk.Separator(p, orient='horizontal').grid(
            row=3, column=0, sticky='ew', pady=(0, 8))

        # --- log output ---
        self._log_box = tk.Text(
            p, height=14, bg='#141414', fg='#c0c0c0',
            font=('Consolas', 9), wrap='word',
            state='disabled', relief='flat', bd=0,
            insertbackground='#c0c0c0',
        )
        self._log_box.grid(row=4, column=0, sticky='nsew')
        sb = ttk.Scrollbar(p, orient='vertical', command=self._log_box.yview)
        sb.grid(row=4, column=1, sticky='ns')
        self._log_box.configure(yscrollcommand=sb.set)

    # -------------------------------------------------------------------------
    # Settings helpers
    # -------------------------------------------------------------------------

    def _load_settings_to_ui(self) -> None:
        self._v_hrd_path.set(   self.cfg.get('hrd_path',    ''))
        self._v_qrz_key.set(    self.cfg.get('qrz_key',     ''))
        self._v_qrz_path.set(   self.cfg.get('qrz_path',    ''))
        self._v_eqsl_user.set(  self.cfg.get('eqsl_user',   ''))
        self._v_eqsl_pass.set(  self.cfg.get('eqsl_pass',   ''))
        self._v_eqsl_path.set(  self.cfg.get('eqsl_path',   ''))
        self._v_hrdlog_call.set(self.cfg.get('hrdlog_call', ''))
        self._v_hrdlog_code.set(self.cfg.get('hrdlog_code', ''))
        self._v_hrdlog_path.set(self.cfg.get('hrdlog_path', ''))
        self._v_gt2_path.set(   self.cfg.get('gt2_path',    ''))

    def _save_settings(self) -> None:
        self.cfg.update({
            'hrd_path':    self._v_hrd_path.get().strip(),
            'qrz_key':     self._v_qrz_key.get().strip(),
            'qrz_path':    self._v_qrz_path.get().strip(),
            'eqsl_user':   self._v_eqsl_user.get().strip(),
            'eqsl_pass':   self._v_eqsl_pass.get().strip(),
            'eqsl_path':   self._v_eqsl_path.get().strip(),
            'hrdlog_call': self._v_hrdlog_call.get().strip(),
            'hrdlog_code': self._v_hrdlog_code.get().strip(),
            'hrdlog_path': self._v_hrdlog_path.get().strip(),
            'gt2_path':    self._v_gt2_path.get().strip(),
        })
        save_config(self.cfg)
        self._log('Settings saved.')

    # -------------------------------------------------------------------------
    # Logging / status
    # -------------------------------------------------------------------------

    def _log(self, msg: str) -> None:
        def _do():
            self._log_box.configure(state='normal')
            ts = datetime.now().strftime('%H:%M:%S')
            self._log_box.insert('end', f'[{ts}] {msg}\n')
            self._log_box.see('end')
            self._log_box.configure(state='disabled')
        self.after(0, _do)

    def _set_status(self, msg: str) -> None:
        self.after(0, lambda: self._v_status.set(f'Status: {msg}'))

    # -------------------------------------------------------------------------
    # Connector factory
    # -------------------------------------------------------------------------

    def _adv(self, key: str):
        """Convenience accessor for advanced config values."""
        return self.cfg.get('advanced', _DEFAULT_ADVANCED).get(key,
               _DEFAULT_ADVANCED[key])

    def _connectors(self) -> dict:
        c = self.cfg
        return {
            'hrd':    LocalHRDConnector(c.get('hrd_path', '')),
            'qrz':    QRZConnector(
                          c.get('qrz_key', ''), c.get('qrz_path', ''),
                          fetch_page_size=self._adv('qrz_fetch_page_size'),
                          push_batch_size=self._adv('qrz_push_batch_size'),
                          timeout_short=self._adv('timeout_short'),
                          timeout_long=self._adv('timeout_long'),
                      ),
            'eqsl':   EQSLConnector(
                          c.get('eqsl_user', ''), c.get('eqsl_pass', ''),
                          c.get('eqsl_path', ''),
                          rcvd_since=self._adv('eqsl_rcvd_since'),
                          timeout_short=self._adv('timeout_short'),
                          timeout_long=self._adv('timeout_long'),
                      ),
            'hrdlog': HRDlogConnector(
                          c.get('hrdlog_call', ''), c.get('hrdlog_code', ''),
                          c.get('hrdlog_path', ''),
                          push_batch_size=self._adv('hrdlog_push_batch_size'),
                          timeout_long=self._adv('timeout_long'),
                      ),
            'gt2':    GridTracker2Connector(c.get('gt2_path', '')),
        }

    # -------------------------------------------------------------------------
    # Fetch
    # -------------------------------------------------------------------------

    def _fetch_all(self) -> None:
        if self._busy:
            return
        self._busy = True
        self._set_status('fetching...')
        self._log('--- Fetch started ---')
        for src in SOURCES:
            self._tree.item(src, values=(SRC_LABELS[src], '-', '...', '-'))
        self._set_buttons('disabled')
        threading.Thread(target=self._fetch_thread, daemon=True).start()

    def _fetch_thread(self) -> None:
        conns = self._connectors()
        for src in SOURCES:
            label = SRC_LABELS[src]
            self._log(f'{label}: fetching...')
            try:
                recs = conns[src].fetch(log=self._log)
                self.engine.store(src, recs)
                # sources return [] when nothing configured — show SKIP not OK
                conn    = conns[src]
                skipped = (not recs and (
                    (src == 'hrdlog' and hasattr(conn, 'adif_path') and not conn.adif_path) or
                    (src == 'eqsl'   and hasattr(conn, 'adif_path') and not conn.adif_path
                                     and hasattr(conn, 'user') and not conn.user) or
                    (src == 'gt2'    and hasattr(conn, 'path') and not conn.path)
                ))
                status = 'SKIP' if skipped else 'OK'
                self.after(0, lambda s=src, n=len(recs), st=status:
                           self._tree.item(s, values=(SRC_LABELS[s], f'{n:,}', st, '-')))
            except Exception as e:
                err_msg = str(e)
                self._log(f'{label}: ERROR — {err_msg}')
                self.after(0, lambda s=src:
                           self._tree.item(s, values=(SRC_LABELS[s], '-', 'ERR', '-')))

        self._log('--- Fetch complete. Computing missing counts... ---')
        self._refresh_missing()
        self._set_status('idle')
        self._busy = False
        self.after(0, lambda: self._set_buttons('normal'))

    def _refresh_missing(self) -> None:
        _notes = {
            'eqsl': ' (received cards only)',
            'gt2':  ' (fetch from ADIF backup)',
        }
        def _do():
            for src in SOURCES:
                cur = self._tree.item(src, 'values')
                if cur[2] == 'OK':
                    note    = _notes.get(src, '')
                    summary = self.engine.missing_summary_for_row(
                        src, self._adv('dedup_window_mins')) + note
                    self._tree.item(src, values=(SRC_LABELS[src], cur[1], cur[2], summary))
        self.after(0, _do)

    # -------------------------------------------------------------------------
    # Push
    # -------------------------------------------------------------------------

    def _set_buttons(self, state: str) -> None:
        for btn in self._btns.values():
            btn.configure(state=state)

    def _push_to(self, target: str) -> None:
        if self._busy:
            return
        records = self.engine.missing_from_target(target, self._adv('dedup_window_mins'))
        label   = SRC_LABELS[target]
        if not records:
            self._log(f'{label}: already in sync — nothing to push.')
            return
        near_idx = _near_dupe_indices(records, self.engine.records[target],
                                      window=self._adv('near_dupe_warn_mins'))

        def _do_push(selected: list) -> None:
            self._busy = True
            self._set_status(f'pushing to {label}...')
            self._set_buttons('disabled')
            conn = self._connectors()[target]
            threading.Thread(
                target=self._push_thread, args=(conn, selected, label), daemon=True
            ).start()

        ReviewDialog(self, records, near_idx, label, on_push=_do_push)

    def _push_thread(self, conn, records: list, label: str) -> None:
        try:
            conn.push(records, log=self._log)
            self._log(f'{label}: push complete.')
        except Exception as e:
            self._log(f'{label}: push ERROR — {e}')
        finally:
            self._busy = False
            self._set_status('idle')
            self.after(0, lambda: self._set_buttons('normal'))

    def _gen_hrd_adif(self) -> None:
        if self._busy:
            return
        records = self.engine.missing_from_target('hrd', self._adv('dedup_window_mins'))
        if not records:
            self._log('HRD Local: already in sync — nothing to generate.')
            return
        near_idx = _near_dupe_indices(records, self.engine.records['hrd'],
                                      window=self._adv('near_dupe_warn_mins'))

        def _do_generate(selected: list) -> None:
            conn = self._connectors()['hrd']
            try:
                out_path = conn.push(selected, log=self._log)
                if out_path:
                    messagebox.showinfo(
                        'HRD Import File Ready',
                        f'Wrote {len(selected):,} records to:\n{out_path}\n\n'
                        'In Ham Radio Deluxe Logbook:\n'
                        '  File -> Import -> ADIF\n'
                        '  Select the file above and import.'
                    )
            except Exception as e:
                self._log(f'HRD generate error: {e}')
                messagebox.showerror('Error', str(e))

        ReviewDialog(self, records, near_idx, 'HRD Local', on_push=_do_generate)

    def _gen_gt2_adif(self) -> None:
        if self._busy:
            return
        records = self.engine.missing_from_target('gt2', self._adv('dedup_window_mins'))
        if not records:
            self._log('GridTracker2: already in sync — nothing to generate.')
            return
        near_idx = _near_dupe_indices(records, self.engine.records['gt2'],
                                      window=self._adv('near_dupe_warn_mins'))

        def _do_generate(selected: list) -> None:
            conn = self._connectors()['gt2']
            try:
                out_path = conn.push(selected, log=self._log)
                if out_path:
                    messagebox.showinfo(
                        'GridTracker2 Import File Ready',
                        f'Wrote {len(selected):,} records to:\n{out_path}\n\n'
                        'To import into GridTracker2:\n'
                        '  Option 1: File -> Import ADIF (if available)\n'
                        '  Option 2: Close GridTracker2, copy this file into\n'
                        '  the Backup Logs folder, then re-open GT2.'
                    )
            except Exception as e:
                self._log(f'GridTracker2 generate error: {e}')
                messagebox.showerror('Error', str(e))

        ReviewDialog(self, records, near_idx, 'GridTracker2', on_push=_do_generate)


# =============================================================================
#  MAIN
# =============================================================================

if __name__ == '__main__':
    app = LogSyncApp()
    app.mainloop()
