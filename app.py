#!/usr/bin/env python3
"""
Ham Radio Companion  —  WSJT-X + PSKReporter mutual-spot finder
================================================================
Setup:
  1. Edit config.ini (same directory) to set your callsign and paths.
  2. pip install requests paho-mqtt
  3. In WSJT-X:  File -> Settings -> Reporting
                 check UDP Server  ->  127.0.0.1 : 2234
  4. python app.py
  5. Click "Load Log CSV" to suppress alerts for already-worked stations.

Expected CSV log columns (case-insensitive, extra columns ignored):
  CALL  or  CALLSIGN  or  STATION_CALLSIGN   -- required
  BAND                                        -- optional  e.g. "20m"
  MODE  or  SUBMODE                           -- optional  e.g. "FT8"

Worked-station matching is per (call, band, mode).  If a CSV row has no
BAND column, it matches any band for that call+mode; same for MODE.
"""

from __future__ import annotations

import configparser
import csv
import os
import queue as _queue_mod
import re
import socket
import struct
import sys
import threading
import time
from datetime import date, datetime, timedelta, timezone
from typing import Optional
import json
import paho.mqtt.client as mqtt
import requests
import tkinter as tk
from tkinter import ttk, filedialog

# =============================================================================
#  CONFIGURATION  — loaded from config.ini (same directory as this script)
# =============================================================================
_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini")

_cfg = configparser.ConfigParser()
if not _cfg.read(_CONFIG_PATH):
    print(f"[CFG] config.ini not found at {_CONFIG_PATH}, using built-in defaults")

def _get(section: str, key: str, fallback):
    try:
        raw = _cfg.get(section, key)
        return type(fallback)(raw) if not isinstance(fallback, str) else raw
    except (configparser.NoSectionError, configparser.NoOptionError):
        return fallback

MY_CALLSIGN          = _get("station",  "callsign",              "NC4MH").upper()
UDP_HOST             = _get("network",  "udp_host",              "0.0.0.0")
UDP_PORT             = _get("network",  "udp_port",              2338)
MQTT_BROKER          = _get("network",  "mqtt_broker",           "138.68.151.174")
MQTT_PORT            = _get("network",  "mqtt_port",             1883)
PSK_HEARD_ME_MAX_AGE = _get("timing",   "psk_heard_me_max_age",  300)
HEARD_EXPIRY         = _get("timing",   "heard_expiry",          600)
MUTUAL_STICKY_SECS   = _get("timing",   "mutual_sticky_secs",    120)
MUTUAL_MAX_AGE_SECS  = _get("timing",   "mutual_max_age_secs",   120)
GUI_REFRESH_MS       = _get("timing",   "gui_refresh_ms",        3000)
LOG_RELOAD_INTERVAL  = _get("timing",   "log_reload_interval",   15)
LOG_FILE             = _get("log_files","hrd_log",               r"C:\Users\micha\OneDrive\Desktop\Documents\Ham Radio Deluxe\HRD Logbook\ExportAll.adi")
JTDX_LOG_FILE        = _get("log_files","jtdx_log",              r"C:\Users\micha\AppData\Local\JTDX\wsjtx_log.ADI")
WORKED_CUTOFF_DAYS   = _get("filter",   "worked_cutoff_days",    730)

# Station profile for propagation
MY_GRID              = _get("station_profile", "grid",         "FM06").upper()
MY_POWER             = _get("station_profile", "power_watts",  100)
MY_ANTENNA           = _get("station_profile", "antenna",      "dipole").lower()

print(f"[CFG] loaded from {_CONFIG_PATH}")
print(f"[CFG] callsign={MY_CALLSIGN}  grid={MY_GRID}  power={MY_POWER}W  antenna={MY_ANTENNA}")
print(f"[CFG] udp={UDP_PORT}  mqtt={MQTT_BROKER}:{MQTT_PORT}  cutoff={WORKED_CUTOFF_DAYS}d")
# =============================================================================


# -- WSJT-X binary protocol ---------------------------------------------------
#
# QDataStream, big-endian.  We handle two message types:
#
#   Type 1 (Status)  ->  gives us the current dial frequency (-> band)
#   Type 2 (Decode)  ->  gives us a decoded callsign message + SNR + mode
#
_WSJTX_MAGIC = 0xADBCCBDA
_MSG_STATUS  = 1
_MSG_DECODE  = 2

_CS_PAT = r'[A-Z0-9]{1,3}[0-9][A-Z0-9]{0,3}[A-Z](?:/[A-Z0-9]+)?'
_CS_RE  = re.compile(rf'\b({_CS_PAT})\b')

_SKIP = frozenset({
    'CQ', 'DE', '73', 'RR73', 'RRR', 'TNX', 'TU', 'AGN',
    'QRZ', 'DX', 'NA', 'EU', 'AS', 'AF', 'OC', 'SA',
    'UP', 'DN', 'PSE', 'HW', 'NR', 'UR', 'FB', 'GL',
})

# Band edge table: (lo_hz, hi_hz, name)
_BAND_MAP = [
    (1_800_000,    2_000_000,  '160m'),
    (3_500_000,    4_000_000,   '80m'),
    (5_000_000,    5_500_000,   '60m'),
    (7_000_000,    7_300_000,   '40m'),
    (10_100_000,  10_150_000,   '30m'),
    (14_000_000,  14_350_000,   '20m'),
    (18_068_000,  18_168_000,   '17m'),
    (21_000_000,  21_450_000,   '15m'),
    (24_890_000,  24_990_000,   '12m'),
    (28_000_000,  29_700_000,   '10m'),
    (50_000_000,  54_000_000,    '6m'),
    (144_000_000, 148_000_000,   '2m'),
]


def _freq_to_band(hz: int) -> str:
    for lo, hi, name in _BAND_MAP:
        if lo <= hz <= hi:
            return name
    return ''


# -- Callsign geo lookup (DXCC country + US state via callook.info) -----------

# Sorted longest-first so greedy prefix matching always picks the best match.
# 3-char entries (e.g. EA8, KH6, IT9) automatically win over shorter ones.
_DXCC_PREFIXES: list = sorted([
    # === United States ===
    ('AA','USA'),('AB','USA'),('AC','USA'),('AD','USA'),('AE','USA'),
    ('AF','USA'),('AG','USA'),('AH','USA'),('AI','USA'),('AJ','USA'),
    ('AK','USA'),('K','USA'),('W','USA'),('N','USA'),
    # US territories (3-char beats single K/W/N/A)
    ('KH6','Hawaii'),('NH6','Hawaii'),('WH6','Hawaii'),('AH6','Hawaii'),
    ('KH2','Guam'),  ('NH2','Guam'),  ('WH2','Guam'),
    ('KL7','Alaska'),('NL7','Alaska'),('WL7','Alaska'),('AL7','Alaska'),
    ('KP4','Puerto Rico'),('NP4','Puerto Rico'),('WP4','Puerto Rico'),
    ('KP2','US V.I.'),  ('NP2','US V.I.'),  ('WP2','US V.I.'),
    # === Canada ===
    ('VE','Canada'),('VA','Canada'),('VY','Canada'),('VO','Canada'),
    # === British Isles ===
    ('2E','England'),('G','England'),('M','England'),
    ('GD','Isle of Man'),('MD','Isle of Man'),
    ('GI','N.Ireland'),('MI','N.Ireland'),
    ('GJ','Jersey'),   ('MJ','Jersey'),
    ('GM','Scotland'), ('MM','Scotland'),
    ('GU','Guernsey'), ('MU','Guernsey'),
    ('GW','Wales'),    ('MW','Wales'),
    ('EI','Ireland'),
    ('ZB','Gibraltar'),
    # === Germany ===
    ('DA','Germany'),('DB','Germany'),('DC','Germany'),('DD','Germany'),
    ('DE','Germany'),('DF','Germany'),('DG','Germany'),('DH','Germany'),
    ('DI','Germany'),('DJ','Germany'),('DK','Germany'),('DL','Germany'),
    ('DM','Germany'),('DN','Germany'),('DO','Germany'),('DP','Germany'),
    ('DQ','Germany'),('DR','Germany'),
    # === France & overseas (2/3-char before single F) ===
    ('FK','New Caledonia'),
    ('FM','Martinique'),
    ('FG','Guadeloupe'),
    ('FO','Fr.Polynesia'),
    ('FR','Reunion'),
    ('FY','Fr.Guiana'),
    ('FH','Mayotte'),
    ('F','France'),('TM','France'),
    # === Italy (IS0/IT9 before single I) ===
    ('IT9','Sicily'),('IS0','Sardinia'),
    ('I','Italy'),
    # === Spain & Islands ===
    ('EA8','Canary Is.'),('EA9','Ceuta/Mel.'),
    ('EA','Spain'),('EB','Spain'),('EC','Spain'),('ED','Spain'),
    ('EE','Spain'),('EF','Spain'),('EG','Spain'),('EH','Spain'),
    # === Portugal ===
    ('CT','Portugal'),('CS','Portugal'),('CU','Azores'),
    # === Netherlands ===
    ('PA','Netherlands'),('PB','Netherlands'),('PC','Netherlands'),
    ('PD','Netherlands'),('PE','Netherlands'),('PF','Netherlands'),
    ('PG','Netherlands'),('PH','Netherlands'),('PI','Netherlands'),
    ('P4','Aruba'),
    # === Belgium ===
    ('ON','Belgium'),('OO','Belgium'),('OP','Belgium'),('OQ','Belgium'),
    ('OR','Belgium'),('OS','Belgium'),('OT','Belgium'),
    # === Scandinavia ===
    ('OX','Greenland'),('OY','Faroe Is.'),('OZ','Denmark'),
    ('LA','Norway'),('LB','Norway'),('LC','Norway'),('LD','Norway'),
    ('LE','Norway'),('LF','Norway'),('LG','Norway'),
    ('OH','Finland'),('OI','Finland'),('OF','Finland'),('OG','Finland'),
    ('SM','Sweden'),('SA','Sweden'),('SB','Sweden'),('SC','Sweden'),
    ('SD','Sweden'),('SE','Sweden'),('SF','Sweden'),('SG','Sweden'),
    ('SH','Sweden'),('SI','Sweden'),('SJ','Sweden'),('SK','Sweden'),
    ('SL','Sweden'),
    ('TF','Iceland'),
    # === Central / Eastern Europe ===
    ('OK','Czech Rep'),('OL','Czech Rep'),
    ('OM','Slovakia'),
    ('OE','Austria'),
    ('HB','Switzerland'),
    ('LX','Luxembourg'),
    ('HA','Hungary'),('HG','Hungary'),
    ('SP','Poland'),('SN','Poland'),('SO','Poland'),('SQ','Poland'),('SR','Poland'),
    ('YO','Romania'),('YP','Romania'),('YQ','Romania'),('YR','Romania'),
    ('LZ','Bulgaria'),
    ('YT','Serbia'),('YU','Serbia'),('YZ','Serbia'),
    ('9A','Croatia'),
    ('S5','Slovenia'),
    ('T9','Bosnia'),
    ('Z3','N.Macedonia'),
    ('ZA','Albania'),
    ('4O','Montenegro'),
    ('Z6','Kosovo'),
    ('ER','Moldova'),
    ('SV9','Crete'),('SV5','Rhodes'),
    ('SV','Greece'),('SW','Greece'),('SX','Greece'),('SY','Greece'),('SZ','Greece'),
    ('5B','Cyprus'),
    ('C3','Andorra'),
    ('3A','Monaco'),
    ('T7','San Marino'),
    ('HV','Vatican'),
    ('9H','Malta'),
    # === Russia ===
    ('UA','Russia'),('UB','Russia'),('UC','Russia'),('UD','Russia'),
    ('UE','Russia'),('UF','Russia'),('UG','Russia'),('UH','Russia'),
    ('UI','Russia'),
    ('RA','Russia'),('RB','Russia'),('RC','Russia'),('RD','Russia'),
    ('RE','Russia'),('RF','Russia'),('RG','Russia'),('RH','Russia'),
    ('RI','Russia'),('RJ','Russia'),('RK','Russia'),('RL','Russia'),
    ('RM','Russia'),('RN','Russia'),('RO','Russia'),('RP','Russia'),
    ('RQ','Russia'),('RR','Russia'),('RS','Russia'),('RT','Russia'),
    ('RU','Russia'),('RV','Russia'),('RW','Russia'),('RX','Russia'),
    ('RY','Russia'),('RZ','Russia'),
    # === Ukraine / Belarus / Baltics / CIS ===
    ('UR','Ukraine'),('US','Ukraine'),('UT','Ukraine'),('UV','Ukraine'),
    ('UW','Ukraine'),('UX','Ukraine'),('UY','Ukraine'),('UZ','Ukraine'),
    ('EW','Belarus'),
    ('ES','Estonia'),('YL','Latvia'),('LY','Lithuania'),
    ('UK','Uzbekistan'),
    ('UN','Kazakhstan'),('UO','Kazakhstan'),('UP','Kazakhstan'),('UQ','Kazakhstan'),
    ('EX','Kyrgyzstan'),('EY','Tajikistan'),
    ('4J','Azerbaijan'),('4K','Azerbaijan'),
    ('4L','Georgia'),
    ('EK','Armenia'),
    # === Middle East ===
    ('TA','Turkey'),('TC','Turkey'),('YM','Turkey'),
    ('4X','Israel'),('4Z','Israel'),
    ('OD','Lebanon'),('YK','Syria'),('YI','Iraq'),
    ('JY','Jordan'),
    ('A9','Bahrain'),('A4','Oman'),('A6','UAE'),('A7','Qatar'),
    ('9K','Kuwait'),('HZ','Saudi Arabia'),('7Z','Saudi Arabia'),
    ('4W','East Timor'),
    # === South Asia ===
    ('VU','India'),('AT','India'),('AU','India'),('AV','India'),('AW','India'),
    ('AP','Pakistan'),('AS','Pakistan'),
    ('S2','Bangladesh'),('S3','Bangladesh'),
    ('4S','Sri Lanka'),
    ('9N','Nepal'),('A5','Bhutan'),
    # === SE Asia ===
    ('HS','Thailand'),('E2','Thailand'),
    ('XU','Cambodia'),
    ('XW','Laos'),
    ('XV','Vietnam'),('3W','Vietnam'),
    ('XY','Myanmar'),('XZ','Myanmar'),
    ('9M','Malaysia'),
    ('9V','Singapore'),
    ('V8','Brunei'),
    ('YB','Indonesia'),('YC','Indonesia'),('YD','Indonesia'),('YE','Indonesia'),
    ('YF','Indonesia'),('YG','Indonesia'),('YH','Indonesia'),
    ('4F','Philippines'),('4G','Philippines'),
    ('DU','Philippines'),('DV','Philippines'),('DW','Philippines'),('DX','Philippines'),
    ('DY','Philippines'),('DZ','Philippines'),
    # === East Asia ===
    ('VR','Hong Kong'),('XX9','Macau'),
    ('BD','China'),('BF','China'),('BG','China'),('BH','China'),('BI','China'),
    ('BJ','China'),('BK','China'),('BL','China'),('BM','China'),('BN','China'),
    ('BO','China'),('BP','China'),('BR','China'),('BS','China'),('BT','China'),
    ('BY','China'),
    ('BV','Taiwan'),('BW','Taiwan'),('BX','Taiwan'),
    ('HL','S.Korea'),('DS','S.Korea'),('DT','S.Korea'),
    ('6K','S.Korea'),('6L','S.Korea'),('6M','S.Korea'),('6N','S.Korea'),
    ('JA','Japan'),('JE','Japan'),('JF','Japan'),('JG','Japan'),
    ('JH','Japan'),('JI','Japan'),('JJ','Japan'),('JK','Japan'),
    ('JL','Japan'),('JM','Japan'),('JN','Japan'),('JO','Japan'),
    ('JP','Japan'),('JQ','Japan'),('JR','Japan'),('JS','Japan'),
    ('7J','Japan'),('7K','Japan'),('7L','Japan'),('7M','Japan'),('7N','Japan'),
    ('JT','Mongolia'),('JV','Mongolia'),
    # === Oceania ===
    ('VK','Australia'),('AX','Australia'),
    ('ZL','New Zealand'),
    ('P2','Papua NG'),
    ('YJ','Vanuatu'),
    ('H4','Solomon Is.'),
    ('3D2','Fiji'),
    ('A3','Tonga'),('5W','Samoa'),('T2','Tuvalu'),('T3','Kiribati'),
    ('V7','Marshall Is.'),('V6','Micronesia'),
    # === Mexico / Central America / Caribbean ===
    ('XE','Mexico'),('XF','Mexico'),('XG','Mexico'),('XH','Mexico'),('XI','Mexico'),
    ('TG','Guatemala'),('TI','Costa Rica'),
    ('HR','Honduras'),('YS','El Salvador'),('YN','Nicaragua'),('HP','Panama'),
    ('HH','Haiti'),('HI','Dom.Rep.'),
    ('CO','Cuba'),('CL','Cuba'),('CM','Cuba'),
    ('C6','Bahamas'),
    ('6Y','Jamaica'),
    ('VP9','Bermuda'),
    ('ZF','Cayman Is.'),
    ('8P','Barbados'),
    ('9Y','Trinidad'),
    ('J3','Grenada'),('J6','St.Lucia'),('J7','Dominica'),('J8','St.Vincent'),
    ('V2','Antigua'),('V4','St.Kitts'),
    ('PJ2','Curacao'),('PJ4','Bonaire'),('PJ7','St.Maarten'),
    # === South America ===
    ('PY','Brazil'),('PP','Brazil'),('PQ','Brazil'),('PR','Brazil'),('PS','Brazil'),
    ('PT','Brazil'),('PU','Brazil'),('PV','Brazil'),('PW','Brazil'),('PX','Brazil'),
    ('LU','Argentina'),('LV','Argentina'),('LW','Argentina'),
    ('CE','Chile'),('CA','Chile'),('CB','Chile'),('CC','Chile'),('CD','Chile'),
    ('OA','Peru'),('OB','Peru'),('OC','Peru'),
    ('HC','Ecuador'),('HD','Ecuador'),
    ('HK','Colombia'),('5J','Colombia'),('5K','Colombia'),
    ('YV','Venezuela'),('YW','Venezuela'),('YX','Venezuela'),('YY','Venezuela'),
    ('ZP','Paraguay'),
    ('CX','Uruguay'),
    ('CP','Bolivia'),
    ('PZ','Suriname'),('8R','Guyana'),
    # === Africa ===
    ('ZS','S.Africa'),('ZT','S.Africa'),('ZU','S.Africa'),
    ('V5','Namibia'),
    ('A2','Botswana'),
    ('Z2','Zimbabwe'),
    ('9J','Zambia'),
    ('5H','Tanzania'),
    ('5Z','Kenya'),
    ('5X','Uganda'),
    ('ET','Ethiopia'),
    ('J2','Djibouti'),
    ('6W','Senegal'),
    ('TJ','Cameroon'),
    ('TT','Chad'),
    ('9G','Ghana'),
    ('5N','Nigeria'),
    ('EL','Liberia'),
    ('C9','Mozambique'),
    ('5R','Madagascar'),
    ('3B','Mauritius'),
    ('ZD','St.Helena'),
    ('7X','Algeria'),
    ('CN','Morocco'),
    ('TS','Tunisia'),('3V','Tunisia'),
    ('SU','Egypt'),
    ('ST','Sudan'),
    ('5A','Libya'),
    ('5U','Niger'),
    ('5T','Mauritania'),
], key=lambda x: -len(x[0]))


def _prefix_country(cs: str) -> str:
    """Return DXCC country for *cs* using longest-prefix matching."""
    base = cs.split('/')[0]          # strip portable suffix (e.g. W1AW/P)
    for pfx, country in _DXCC_PREFIXES:
        if base.startswith(pfx):
            return country
    return ''


def _callook_state(cs: str) -> str:
    """Query callook.info for the US state of *cs*. Returns 2-letter state abbrev or ''."""
    try:
        r = requests.get(f"https://callook.info/{cs}/json", timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data.get('status') == 'VALID':
                # address.line2 format: "CITY, ST ZIPCODE"  e.g. "TUCSON, AZ 85748"
                line2 = data.get('address', {}).get('line2', '')
                if ',' in line2:
                    after_comma = line2.split(',', 1)[1].strip()   # "AZ 85748"
                    parts = after_comma.split()
                    if parts and len(parts[0]) == 2 and parts[0].isalpha():
                        return parts[0]
    except Exception:
        pass
    return ''


# Module-level lookup cache and queue
_lookup_cache: dict               = {}                    # cs -> {'state': str, 'country': str}
_cache_lock                       = threading.Lock()
_lookup_queue: _queue_mod.Queue   = _queue_mod.Queue()
_lookup_queued: set               = set()                 # cs already in the queue
_lookup_queued_lock               = threading.Lock()


class _Buf:
    """Minimal big-endian binary reader for WSJT-X datagrams."""

    def __init__(self, data: bytes) -> None:
        self._d = data
        self._o = 0

    def u32(self) -> int:
        v, = struct.unpack_from('>I', self._d, self._o)
        self._o += 4
        return v

    def u64(self) -> int:
        v, = struct.unpack_from('>Q', self._d, self._o)
        self._o += 8
        return v

    def i32(self) -> int:
        v, = struct.unpack_from('>i', self._d, self._o)
        self._o += 4
        return v

    def u8(self) -> int:
        v = self._d[self._o]
        self._o += 1
        return v

    def bool_(self) -> bool:
        return bool(self.u8())

    def f64(self) -> float:
        v, = struct.unpack_from('>d', self._d, self._o)
        self._o += 8
        return v

    def utf8(self) -> str:
        n = self.u32()
        if n == 0xFFFF_FFFF:          # Qt null string
            return ''
        s = self._d[self._o:self._o + n].decode('utf-8', errors='replace')
        self._o += n
        return s


def _parse_wsjtx(data: bytes) -> Optional[dict]:
    """
    Parse a WSJT-X UDP packet.

    Returns one of:
      {'type': _MSG_DECODE, 'snr': int, 'mode': str, 'message': str}
      {'type': _MSG_STATUS, 'freq': int}   # dial frequency in Hz
      None  -- unrecognised or malformed
    """
    try:
        b = _Buf(data)
        if b.u32() != _WSJTX_MAGIC:
            return None
        b.u32()                       # schema version
        msg_type = b.u32()

        if msg_type == _MSG_DECODE:
            b.utf8()                  # instance id
            b.bool_()                 # new decode flag
            b.u32()                   # time ms from midnight UTC
            snr  = b.i32()
            b.f64()                   # delta-t (s)
            b.u32()                   # delta-f (Hz)
            mode = b.utf8()
            msg  = b.utf8()
            return {'type': _MSG_DECODE, 'snr': snr, 'mode': mode, 'message': msg}

        if msg_type == _MSG_STATUS:
            b.utf8()                  # instance id
            freq = b.u64()            # dial frequency (Hz)
            mode = b.utf8()           # current mode e.g. "FT8"
            return {'type': _MSG_STATUS, 'freq': freq, 'mode': mode}

        return None
    except Exception:
        return None


def _callsigns_in(message: str) -> list:
    """Extract valid callsigns from a decoded FT8/JT65 message."""
    found = []
    for part in message.upper().split():
        part = part.strip('<>[]')
        if part in _SKIP:
            continue
        if _CS_RE.fullmatch(part):
            found.append(part)
    return found


# -- Log file loaders (ADIF + CSV) --------------------------------------------

def _parse_adif_records(content: str) -> list:
    """
    Parse raw ADIF text into a list of {FIELD: value} dicts.
    Handles the length-prefixed tag format: <FIELDNAME:LENGTH>value
    and optional type specifier:            <FIELDNAME:LENGTH:TYPE>value
    Skips everything before <EOH> (file header).
    """
    eoh = re.search(r'<EOH>', content, re.IGNORECASE)
    if eoh:
        content = content[eoh.end():]

    tag_re = re.compile(r'<(\w+)(?::(\d+)(?::\w+)?)?>',  re.IGNORECASE)
    records, current, pos = [], {}, 0

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


def _parse_qso_date(date_s: str) -> date:
    """Parse ADIF QSO_DATE (YYYYMMDD) to a date. Returns today on failure."""
    try:
        return date(int(date_s[:4]), int(date_s[4:6]), int(date_s[6:8]))
    except Exception:
        return date.today()


def _load_log_adif(path: str) -> dict:
    """
    Load an ADIF (.adi / .adif) contact log.
    Returns a dict of (call, band, mode) -> most_recent_date.
    Uses ADIF fields: CALL, BAND, MODE, QSO_DATE.
    """
    contacts: dict = {}
    try:
        with open(path, encoding='utf-8-sig', errors='replace') as fh:
            content = fh.read()
        for rec in _parse_adif_records(content):
            call = rec.get('CALL', '').strip().upper()
            band = rec.get('BAND', '').strip().lower()
            mode = rec.get('MODE', '').strip().upper()
            qso_date = _parse_qso_date(rec.get('QSO_DATE', ''))
            if call:
                key = (call, band, mode)
                if key not in contacts or contacts[key] < qso_date:
                    contacts[key] = qso_date
        print(f"[LOG] Parsed {len(contacts)} ADIF contact(s)")
    except Exception as exc:
        print(f"[LOG] Error reading ADIF {path}: {exc}")
    return contacts


def _load_log_csv(path: str) -> dict:
    """
    Load a CSV contact log.  Returns a dict of (call, band, mode) -> most_recent_date.
    Recognised column name variants (case-insensitive):
      call : CALL, CALLSIGN, STATION_CALLSIGN
      band : BAND
      mode : MODE, SUBMODE
      date : QSO_DATE, DATE
    """
    contacts: dict = {}
    try:
        with open(path, newline='', encoding='utf-8-sig') as fh:
            reader = csv.DictReader(fh)
            if reader.fieldnames is None:
                return contacts
            norm = {f.strip().upper(): f for f in reader.fieldnames}
            call_col = (norm.get('CALL')
                        or norm.get('CALLSIGN')
                        or norm.get('STATION_CALLSIGN'))
            band_col = norm.get('BAND')
            mode_col = norm.get('MODE') or norm.get('SUBMODE')
            date_col = norm.get('QSO_DATE') or norm.get('DATE')
            if call_col is None:
                print(f"[LOG] No callsign column found in {path}")
                return contacts
            for row in reader:
                call = row.get(call_col, '').strip().upper()
                band = row.get(band_col, '').strip().lower() if band_col else ''
                mode = row.get(mode_col, '').strip().upper() if mode_col else ''
                qso_date = _parse_qso_date(row.get(date_col, '')) if date_col else date.today()
                if call:
                    key = (call, band, mode)
                    if key not in contacts or contacts[key] < qso_date:
                        contacts[key] = qso_date
    except Exception as exc:
        print(f"[LOG] Error reading CSV {path}: {exc}")
    return contacts


def _load_log_file(path: str) -> dict:
    """Dispatch to ADIF or CSV loader based on file extension."""
    ext = os.path.splitext(path)[1].lower()
    if ext in ('.adi', '.adif'):
        return _load_log_adif(path)
    return _load_log_csv(path)


def _is_worked(cs: str, band: str, mode: str, logged: dict, cutoff: date) -> bool:
    """
    True if cs was logged on this band (any mode) OR this mode (any band)
    within the cutoff window.  This matches JTDX filtering behavior —
    stations already worked on either the current band or current mode
    are hidden.
    """
    b = band.lower()
    m = mode.upper()
    for (c, lb, lm), d in logged.items():
        if c != cs:
            continue
        if d < cutoff:
            continue
        # Worked on this band (any mode) or this mode (any band)
        if lb == b or lm == m:
            return True
    return False



# -- Shared application state -------------------------------------------------

class AppState:
    """
    Thread-safe data store.

    Worker threads write via record_heard(), set_band(), update_spotted_by().
    GUI thread reads via snapshot() and writes via load_log(), expire_heard().
    _prev_mutual is GUI-thread-only and needs no lock.
    """

    def __init__(self) -> None:
        self._lock             = threading.Lock()
        self.heard: dict       = {}        # cs -> {snr, mode, time}
        self.spotted_by: dict  = {}        # cs -> {snr, band, mode} from PSKReporter
        self.logged: dict      = {}        # (call, band, mode) -> most_recent_date
        self.log_path: str     = ''
        self.current_band: str = ''
        self.current_mode: str = ''
        self.last_psk: Optional[datetime] = None
        self.mqtt_connected: bool          = False
        self._prev_mutual: set = set()     # GUI-thread only

        # Propagation engine
        from propagation import PropagationEngine, ANTENNA_DIPOLE, ANTENNA_VERTICAL, ANTENNA_YAGI_3
        _ant_map = {"dipole": ANTENNA_DIPOLE, "vertical": ANTENNA_VERTICAL, "yagi": ANTENNA_YAGI_3}
        _ant_code = _ant_map.get(MY_ANTENNA, ANTENNA_DIPOLE)
        self.prop_engine = PropagationEngine(MY_GRID, int(MY_POWER), _ant_code)
        self.prop_engine.start()

        # Contact probability engine (with propagation)
        from predictor import ContactPredictor
        self.predictor = ContactPredictor(MY_CALLSIGN, prop_engine=self.prop_engine)

    # -- worker-thread writers -------------------------------------------------

    def record_heard(self, cs: str, snr: int, mode: str) -> None:
        with self._lock:
            self.heard[cs] = {
                'snr':  snr,
                'mode': mode,
                'time': datetime.now(timezone.utc),
            }

    def set_band_mode(self, band: str, mode: str) -> None:
        with self._lock:
            if band:
                self.current_band = band
                self.predictor.set_band(band)
            if mode:
                self.current_mode = mode

    def add_spot(self, cs: str, snr: int, band: str, mode: str, ts: float) -> None:
        with self._lock:
            existing = self.spotted_by.get(cs)
            if existing is None or ts >= existing['time']:
                self.spotted_by[cs] = {'snr': snr, 'band': band, 'mode': mode, 'time': ts}
                self.last_psk = datetime.now(timezone.utc)

    def expire_spots(self) -> None:
        cutoff = time.time() - PSK_HEARD_ME_MAX_AGE
        with self._lock:
            stale = [cs for cs, v in self.spotted_by.items() if v['time'] < cutoff]
            for cs in stale:
                del self.spotted_by[cs]

    def set_mqtt_connected(self, connected: bool) -> None:
        with self._lock:
            self.mqtt_connected = connected

    # -- GUI-thread writers ----------------------------------------------------

    def expire_heard(self) -> None:
        cutoff = time.time() - HEARD_EXPIRY
        with self._lock:
            stale = [k for k, v in self.heard.items()
                     if v['time'].timestamp() < cutoff]
            for k in stale:
                del self.heard[k]

    def set_logged(self, contacts: dict, desc: str) -> None:
        """Set the merged contact dict from the log worker."""
        with self._lock:
            self.logged   = contacts
            self.log_path = desc

    def load_log(self, path: str) -> int:
        """Load ADIF or CSV log manually; returns number of contacts."""
        contacts = _load_log_file(path)
        with self._lock:
            self.logged   = contacts
            self.log_path = path
        return len(contacts)

    # -- GUI-thread reader -----------------------------------------------------

    def snapshot(self) -> tuple:
        with self._lock:
            h    = dict(self.heard)
            s    = dict(self.spotted_by)
            p    = self.last_psk
            log  = dict(self.logged)
            band = self.current_band
            mode = self.current_mode
            conn = self.mqtt_connected
        mutual     = {cs for cs in h if cs in s}
        new_mutual = mutual - self._prev_mutual
        self._prev_mutual = mutual
        return h, s, mutual, new_mutual, p, log, band, mode, conn


# -- Background workers -------------------------------------------------------

def _udp_worker(state: AppState) -> None:
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((UDP_HOST, UDP_PORT))
    except OSError as exc:
        print(f"[UDP] FATAL -- cannot bind to :{UDP_PORT}: {exc}")
        return

    sock.settimeout(1.0)
    print(f"[UDP] listening on {UDP_HOST}:{UDP_PORT}")

    while True:
        try:
            data, _ = sock.recvfrom(65535)
        except socket.timeout:
            continue
        pkt = _parse_wsjtx(data)
        if pkt is None:
            continue
        if pkt['type'] == _MSG_DECODE:
            # JTDX sends '~' as a placeholder in Decode packets — ignore it
            # and use the authoritative mode from the last Status packet
            raw = pkt['mode']
            mode = (raw if raw and raw != '~' else None) or state.current_mode
            for cs in _callsigns_in(pkt['message']):
                if cs != MY_CALLSIGN:
                    state.record_heard(cs, pkt['snr'], mode)
            # Feed raw message to contact predictor for activity tracking
            try:
                state.predictor.update_from_decode(pkt['message'], pkt['snr'])
            except Exception:
                pass
        elif pkt['type'] == _MSG_STATUS:
            state.set_band_mode(_freq_to_band(pkt['freq']), pkt['mode'])


def _mqtt_worker(state: AppState) -> None:
    topic = f"pskr/filter/v2/+/+/{MY_CALLSIGN}/#"

    def on_connect(client, userdata, flags, reason_code, properties):
        print(f"[MQTT] connected  topic={topic}")
        client.subscribe(topic)
        state.set_mqtt_connected(True)

    def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
        print(f"[MQTT] disconnected: {reason_code}")
        state.set_mqtt_connected(False)

    def on_message(client, userdata, msg):
        try:
            data = json.loads(msg.payload)
            cs   = data.get('rc', '').strip().upper()
            snr  = int(data.get('rp') or 0)
            band = data.get('b', '')
            mode = data.get('md', '')
            ts   = float(data.get('t', time.time()))
            if cs:
                state.add_spot(cs, snr, band, mode, ts)
        except Exception as exc:
            print(f"[MQTT] parse error: {exc}")

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.on_message    = on_message

    while True:
        try:
            print(f"[MQTT] connecting to {MQTT_BROKER}:{MQTT_PORT} ...")
            client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            client.loop_forever(retry_first_connection=True)
        except Exception as exc:
            print(f"[MQTT] connection error: {exc}")
            state.set_mqtt_connected(False)
            time.sleep(10)


def _log_worker(state: AppState) -> None:
    """Watch JTDX and HRD log files by mtime; merge and update state when either changes."""
    hrd_mtime:  float = 0.0
    jtdx_mtime: float = 0.0
    hrd_contacts:  dict = {}
    jtdx_contacts: dict = {}

    while True:
        changed = False

        if LOG_FILE and os.path.exists(LOG_FILE):
            mt = os.path.getmtime(LOG_FILE)
            if mt != hrd_mtime:
                hrd_contacts = _load_log_file(LOG_FILE)
                hrd_mtime    = mt
                print(f"[LOG] HRD reloaded: {len(hrd_contacts)} contacts")
                changed = True
        elif LOG_FILE and hrd_mtime == 0.0:
            print(f"[LOG] HRD file not found: {LOG_FILE}")

        if JTDX_LOG_FILE and os.path.exists(JTDX_LOG_FILE):
            mt = os.path.getmtime(JTDX_LOG_FILE)
            if mt != jtdx_mtime:
                jtdx_contacts = _load_log_file(JTDX_LOG_FILE)
                jtdx_mtime    = mt
                print(f"[LOG] JTDX reloaded: {len(jtdx_contacts)} contacts")
                changed = True
        elif JTDX_LOG_FILE and jtdx_mtime == 0.0:
            print(f"[LOG] JTDX file not found: {JTDX_LOG_FILE}")

        if changed:
            merged = dict(hrd_contacts)
            for key, d in jtdx_contacts.items():
                if key not in merged or d > merged[key]:
                    merged[key] = d
            desc = f"JTDX({len(jtdx_contacts)}) + HRD({len(hrd_contacts)})"
            state.set_logged(merged, desc)
            print(f"[LOG] Merged: {len(merged)} contacts")

        time.sleep(LOG_RELOAD_INTERVAL)


def _lookup_worker() -> None:
    """Daemon: resolves callsign country (offline) + US state (callook.info API)."""
    while True:
        cs = _lookup_queue.get()
        try:
            country = _prefix_country(cs)
            state   = _callook_state(cs) if country == 'USA' else ''
            with _cache_lock:
                _lookup_cache[cs] = {'state': state, 'country': country}
            print(f"[LOOK] {cs}  country={country or '?'}  state={state or '-'}")
        except Exception as exc:
            print(f"[LOOK] error for {cs}: {exc}")
        finally:
            _lookup_queue.task_done()
            with _lookup_queued_lock:
                _lookup_queued.discard(cs)
        time.sleep(0.3)      # rate-limit callook.info requests


# -- Colour palette -----------------------------------------------------------
C = dict(
    bg          = '#1e1e2e',
    green_bg    = '#0c250c',
    green_fg    = '#39ff7a',
    hdr         = '#7878a8',
    text        = '#d8d8f0',
    bar_bg      = '#12121e',
)


# -- GUI ----------------------------------------------------------------------

class HamApp(tk.Tk):

    def __init__(self, state: AppState) -> None:
        super().__init__()
        self.state = state
        self.title(f"{MY_CALLSIGN} — Ham Radio Companion")
        self.configure(bg=C['bg'])
        self.minsize(960, 560)
        self._build_styles()
        self._build_ui()
        self._sticky: dict = {}   # cs -> datetime last seen as mutual
        self.after(GUI_REFRESH_MS, self._refresh_loop)

    # -- ttk styles ------------------------------------------------------------

    def _build_styles(self) -> None:
        s = ttk.Style(self)
        s.theme_use('clam')

        s.configure('Mutual.Treeview',
                    background=C['green_bg'], foreground=C['green_fg'],
                    fieldbackground=C['green_bg'], rowheight=24,
                    font=('Courier', 10, 'bold'))
        s.configure('Mutual.Treeview.Heading',
                    background='#0a1e0a', foreground=C['green_fg'],
                    font=('Courier', 9, 'bold'), relief='flat')
        s.map('Mutual.Treeview',
              background=[('selected', '#226622')],
              foreground=[('selected', '#ffffff')])

        # Contact Probability panel styles
        s.configure('Prob.Treeview',
                    background='#0e0e1e', foreground='#c0c8e0',
                    fieldbackground='#0e0e1e', rowheight=22,
                    font=('Courier', 9))
        s.configure('Prob.Treeview.Heading',
                    background='#14142a', foreground='#00b4d8',
                    font=('Courier', 9, 'bold'), relief='flat')
        s.map('Prob.Treeview',
              background=[('selected', '#1a3a5a')],
              foreground=[('selected', '#ffffff')])

    # -- layout ----------------------------------------------------------------

    def _build_ui(self) -> None:
        # status strip
        bar = tk.Frame(self, bg=C['bar_bg'], pady=4)
        bar.pack(fill='x', side='top')

        self._udp_lbl = tk.Label(
            bar, text=f"  UDP :{UDP_PORT}  listening",
            bg=C['bar_bg'], fg='#44cc44', font=('Courier', 9))
        self._udp_lbl.pack(side='left', padx=(8, 4))

        self._heard_lbl = tk.Label(
            bar, text='heard: 0  |  psk: 0',
            bg=C['bar_bg'], fg=C['hdr'], font=('Courier', 9))
        self._heard_lbl.pack(side='left', padx=(8, 4))

        tk.Button(
            bar, text='Load Log CSV',
            bg='#2a2a4a', fg=C['text'],
            activebackground='#3a3a6a', activeforeground='#ffffff',
            font=('Courier', 9), relief='flat', padx=8, pady=2,
            command=self._load_log_dialog,
        ).pack(side='left', padx=4)

        init_text = os.path.basename(LOG_FILE) if LOG_FILE else 'no log configured'
        self._log_lbl = tk.Label(
            bar, text=init_text,
            bg=C['bar_bg'], fg=C['hdr'], font=('Courier', 9))
        self._log_lbl.pack(side='left', padx=4)

        self._solar_lbl = tk.Label(
            bar, text='Solar: loading...',
            bg=C['bar_bg'], fg=C['hdr'], font=('Courier', 9))
        self._solar_lbl.pack(side='right', padx=8)

        self._psk_lbl = tk.Label(
            bar, text='MQTT  --  connecting ...',
            bg=C['bar_bg'], fg=C['hdr'], font=('Courier', 9))
        self._psk_lbl.pack(side='right', padx=12)

        # -- Mutual Spots panel ------------------------------------------------
        mf = tk.LabelFrame(
            self,
            text='  *  Mutual Spots  --  I hear them  &  they hear me  *  ',
            bg=C['green_bg'], fg=C['green_fg'],
            font=('Courier', 10, 'bold'), relief='groove', bd=2)
        mf.pack(fill='both', expand=True, padx=8, pady=(4, 0))

        self._mutual_count = tk.StringVar(value='0 mutual spots')
        tk.Label(mf, textvariable=self._mutual_count,
                 bg=C['green_bg'], fg=C['green_fg'], font=('Courier', 8)
                 ).pack(anchor='e', padx=8)

        self._mtree = ttk.Treeview(
            mf,
            columns=('callsign', 'snr', 'hears_me', 'last_heard', 'heard_me', 'band', 'mode', 'state', 'country'),
            show='headings', selectmode='none', style='Mutual.Treeview')
        for col, lbl, w in [
            ('callsign',   'Callsign',        125),
            ('snr',        'SNR (dB)',          80),
            ('hears_me',   'Hears Me',          90),
            ('last_heard', 'Heard Callsign',   125),
            ('heard_me',   'PSK Heard Me',      125),
            ('band',       'Band',              70),
            ('mode',       'Mode',              80),
            ('state',      'State',             65),
            ('country',    'Country',          135),
        ]:
            self._mtree.heading(col, text=lbl)
            self._mtree.column(col, width=w, anchor='center', stretch=True)


        msb = ttk.Scrollbar(mf, orient='vertical', command=self._mtree.yview)
        self._mtree.configure(yscrollcommand=msb.set)
        self._mtree.pack(side='left', fill='both', expand=True,
                         padx=(4, 0), pady=(0, 4))
        msb.pack(side='right', fill='y', pady=(0, 4), padx=(0, 4))

        # -- Contact Probability panel -------------------------------------------
        pf = tk.LabelFrame(
            self,
            text='  Contact Probability  --  Who should I call?  ',
            bg='#0e0e1e', fg='#00b4d8',
            font=('Courier', 10, 'bold'), relief='groove', bd=2)
        pf.pack(fill='both', expand=True, padx=8, pady=(4, 8))

        self._prob_count = tk.StringVar(value='Analyzing...')
        tk.Label(pf, textvariable=self._prob_count,
                 bg='#0e0e1e', fg='#00b4d8', font=('Courier', 8)
                 ).pack(anchor='e', padx=8)

        self._ptree = ttk.Treeview(
            pf,
            columns=('rank', 'callsign', 'score', 'confidence', 'status',
                     'snr_fwd', 'snr_rev', 'country', 'state', 'recommendation'),
            show='headings', selectmode='none', style='Prob.Treeview')
        for col, lbl, w in [
            ('rank',           '#',              35),
            ('callsign',       'Call',            85),
            ('score',          'Scr',             45),
            ('confidence',     'Conf',            75),
            ('status',         'Status',          85),
            ('snr_fwd',        'S>',              45),
            ('snr_rev',        '<S',              45),
            ('country',        'DXCC',            80),
            ('state',          'St',              40),
            ('recommendation', 'Recommendation', 450),
        ]:
            self._ptree.heading(col, text=lbl)
            stretch = (col == 'recommendation')
            self._ptree.column(col, width=w, anchor='center', stretch=stretch, minwidth=w)
        # Left-align recommendation and let it stretch
        self._ptree.column('recommendation', anchor='w', stretch=True)

        psb = ttk.Scrollbar(pf, orient='vertical', command=self._ptree.yview)
        self._ptree.configure(yscrollcommand=psb.set)
        self._ptree.pack(side='left', fill='both', expand=True,
                         padx=(4, 0), pady=(0, 4))
        psb.pack(side='right', fill='y', pady=(0, 4), padx=(0, 4))

        # Tag colors for confidence levels
        self._ptree.tag_configure('HIGH',     foreground='#2ecc71')
        self._ptree.tag_configure('GOOD',     foreground='#00b4d8')
        self._ptree.tag_configure('MODERATE', foreground='#f39c12')
        self._ptree.tag_configure('LOW',      foreground='#e67e22')
        self._ptree.tag_configure('UNLIKELY', foreground='#666688')
        # Active connection — bright white on green background
        self._ptree.tag_configure('ACTIVE',   foreground='#ffffff',
                                  background='#1a6b1a')

    # -- log file dialog -------------------------------------------------------

    def _load_log_dialog(self) -> None:
        path = filedialog.askopenfilename(
            title='Select Logged Contacts CSV',
            filetypes=[
                ('ADIF log files', '*.adi *.adif'),
                ('CSV files',      '*.csv'),
                ('All files',      '*.*'),
            ],
        )
        if not path:
            return
        n = self.state.load_log(path)
        fname = os.path.basename(path)
        self._log_lbl.config(
            text=f"{fname}  ({n} contact{'s' if n != 1 else ''})",
            fg='#44cc44')
        print(f"[LOG] Loaded {n} contact(s) from {fname}")

    # -- periodic refresh ------------------------------------------------------

    def _refresh_loop(self) -> None:
        try:
            self._do_refresh()
        finally:
            self.after(GUI_REFRESH_MS, self._refresh_loop)

    def _do_refresh(self) -> None:
        self.state.expire_heard()
        self.state.expire_spots()
        heard, spotted_by, mutual, new_mutual, last_psk, logged, band, cur_mode, mqtt_conn = \
            self.state.snapshot()

        has_log = bool(logged)
        cutoff  = (datetime.now(timezone.utc) - timedelta(days=WORKED_CUTOFF_DAYS)).date()

        # -- mutual treeview ---------------------------------------------------
        now = datetime.now(timezone.utc)

        # refresh sticky timestamps for currently active mutual spots
        for cs in mutual:
            self._sticky[cs] = now

        # purge entries that have been gone longer than MUTUAL_STICKY_SECS
        self._sticky = {
            cs: t for cs, t in self._sticky.items()
            if (now - t).total_seconds() < MUTUAL_STICKY_SECS
        }

        self._mtree.delete(*self._mtree.get_children())
        displayed = 0

        # Sort by: most recently heard first, then most recent PSK Heard Me
        def _mutual_sort_key(cs):
            info = heard.get(cs, {})
            h_time = info.get('time')
            # Age in seconds — smaller = more recent = sorts first
            heard_age = (now - h_time).total_seconds() if h_time else 99999
            # PSK spot age — smaller = more recent = sorts first
            spotter = spotted_by.get(cs, {})
            psk_ts = spotter.get('time', 0)
            psk_age = (now.timestamp() - psk_ts) if psk_ts else 99999
            return (heard_age, psk_age)

        by_heard = sorted(self._sticky, key=_mutual_sort_key)
        for cs in by_heard:
            info      = heard.get(cs, {})
            heard_time = info.get('time')
            # drop if not recently heard
            if heard_time is None:
                continue
            if (now - heard_time).total_seconds() > MUTUAL_MAX_AGE_SECS:
                continue
            mode   = info.get('mode') or cur_mode or '?'
            # skip stations already in the log for this band+mode
            if has_log and _is_worked(cs, band, mode, logged, cutoff):
                continue
            snr_s  = f"{info['snr']:+d}" if isinstance(info.get('snr'), int) else '?'
            t_s    = f"{int((now - info['time']).total_seconds())}s ago" if 'time' in info else '?'

            # geo lookup — compute country instantly; queue US callsigns for state
            with _cache_lock:
                geo = _lookup_cache.get(cs)
            if geo is None:
                country = _prefix_country(cs)
                geo = {'state': '', 'country': country}
                with _cache_lock:
                    _lookup_cache[cs] = geo
                if country == 'USA':
                    with _lookup_queued_lock:
                        if cs not in _lookup_queued:
                            _lookup_queued.add(cs)
                            _lookup_queue.put(cs)

            state_s   = geo.get('state', '')
            country_s = geo.get('country', '')
            spotter   = spotted_by.get(cs, {})
            hears_snr = spotter.get('snr')
            hears_s   = f"{hears_snr:+d}" if isinstance(hears_snr, int) else '?'
            psk_ts    = spotter.get('time', 0)
            if psk_ts:
                age_s = int(now.timestamp() - psk_ts)
                if age_s > PSK_HEARD_ME_MAX_AGE:
                    continue                       # too stale — skip this entry
                m, s   = divmod(age_s, 60)
                heard_me_s = f"{m}m {s:02d}s ago" if m else f"{s}s ago"
            else:
                heard_me_s = '?'
            self._mtree.insert('', 'end',
                               values=(cs, snr_s, hears_s, t_s, heard_me_s, band or '?', mode,
                                       state_s, country_s))
            displayed += 1
        self._mutual_count.set(f"{displayed} new mutual spot{'s' if displayed != 1 else ''}")

        # -- Contact Probability panel -------------------------------------------
        try:
            self.state.predictor.expire_activity()
            def _country_for(cs):
                with _cache_lock:
                    geo = _lookup_cache.get(cs)
                if geo:
                    return geo.get('country', '')
                return _prefix_country(cs)

            rankings = self.state.predictor.rank_stations(
                heard, spotted_by, logged, band, cur_mode, top_n=20,
                country_lookup=_country_for)

            self._ptree.delete(*self._ptree.get_children())
            for rank, entry in enumerate(rankings, 1):
                cs = entry['callsign']
                # Geo lookup (reuse cache from mutual panel)
                with _cache_lock:
                    geo = _lookup_cache.get(cs)
                if geo is None:
                    country = _prefix_country(cs)
                    geo = {'state': '', 'country': country}
                    with _cache_lock:
                        _lookup_cache[cs] = geo
                    if country == 'USA':
                        with _lookup_queued_lock:
                            if cs not in _lookup_queued:
                                _lookup_queued.add(cs)
                                _lookup_queue.put(cs)

                fwd = f"{entry['heard_snr']:+d}" if entry.get('heard_snr') is not None else '-'
                rev = f"{entry['spot_snr']:+d}" if entry.get('spot_snr') is not None else '-'
                conf = entry['confidence']

                # Use ACTIVE tag for stations in direct contact with us
                tag = 'ACTIVE' if entry['score'] >= 99 else conf

                self._ptree.insert('', 'end',
                    values=(
                        rank,
                        cs,
                        entry['score'],
                        conf if entry['score'] < 99 else 'ACTIVE',
                        entry['state'],
                        fwd,
                        rev,
                        geo.get('country', ''),
                        geo.get('state', ''),
                        entry['recommendation'],
                    ),
                    tags=(tag,))

            n = len(rankings)
            high_n = sum(1 for r in rankings if r['confidence'] == 'HIGH')
            good_n = sum(1 for r in rankings if r['confidence'] == 'GOOD')
            self._prob_count.set(
                f"{n} station{'s' if n != 1 else ''}  |  "
                f"{high_n} HIGH  {good_n} GOOD")
        except Exception as exc:
            self._prob_count.set(f"Predictor error: {exc}")

        # -- log label (contact count + source desc) ---------------------------
        if has_log:
            with self.state._lock:
                n_log = len(self.state.logged)
                lpath = self.state.log_path
            # log_path may be a merged desc like "JTDX(42) + HRD(13566)"
            # or a raw file path from the manual load dialog
            if os.sep in lpath or (len(lpath) > 30 and '(' not in lpath):
                fname = os.path.basename(lpath)
            else:
                fname = lpath
            self._log_lbl.config(
                text=f"{fname}  ({n_log} contacts)",
                fg='#44cc44')

        # -- MQTT status -------------------------------------------------------
        if mqtt_conn:
            n_s    = len(spotted_by)
            last_s = f"  last {last_psk.strftime('%H:%M:%S')} UTC" if last_psk else ''
            self._psk_lbl.config(
                text=f"MQTT live{last_s}  |  {n_s} spotter{'s' if n_s != 1 else ''}",
                fg='#44cc44')
        else:
            self._psk_lbl.config(text='MQTT  --  reconnecting ...', fg='#cc4444')

        # -- heard / psk debug counter -----------------------------------------
        self._heard_lbl.config(
            text=f"heard: {len(heard)}  |  psk: {len(spotted_by)}")

        # -- solar conditions display ------------------------------------------
        solar = self.state.prop_engine.get_solar_summary()
        if solar.get("sfi", 0) > 0:
            sfi = solar["sfi"]
            k = solar.get("k_index", 0)
            geo = solar.get("geomagfield", "")
            # Color based on K-index
            if k <= 2:
                sol_color = "#44cc44"  # Green — quiet
            elif k <= 3:
                sol_color = "#f0c040"  # Yellow — unsettled
            else:
                sol_color = "#cc4444"  # Red — active/storm
            # Band condition for current freq
            bands = solar.get("band_conditions", {})
            bcond = ""
            if band:
                from propagation import _band_to_freq, _freq_to_band_group
                freq = _band_to_freq(band)
                grp = _freq_to_band_group(freq)
                if grp and grp in bands:
                    bcond = f" | {grp}: {bands[grp].get('day', '?')}"
            self._solar_lbl.config(
                text=f"SFI={sfi} K={k} {geo}{bcond}",
                fg=sol_color)
        else:
            self._solar_lbl.config(text="Solar: waiting...", fg=C['hdr'])

        # -- band in title bar -------------------------------------------------
        if band:
            self.title(f"{MY_CALLSIGN} — Ham Radio Companion  [{band}]  {MY_GRID}")





# -- Entry point --------------------------------------------------------------

def main() -> None:
    if MY_CALLSIGN == 'YOUR_CALLSIGN':
        print('+----------------------------------------------+')
        print('|  ERROR: open app.py and set MY_CALLSIGN.    |')
        print('+----------------------------------------------+')
        sys.exit(1)

    state = AppState()
    threading.Thread(target=_udp_worker,   args=(state,), daemon=True).start()
    threading.Thread(target=_mqtt_worker,  args=(state,), daemon=True).start()
    threading.Thread(target=_log_worker,   args=(state,), daemon=True).start()
    threading.Thread(target=_lookup_worker,               daemon=True).start()

    app = HamApp(state)
    app.mainloop()


if __name__ == '__main__':
    main()
