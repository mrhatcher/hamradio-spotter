"""Contact Probability Engine for Ham Radio Spotter.

Parses FT8/FT4 decoded messages, tracks per-station activity state,
and computes a probability score indicating how likely a station is
to respond if called right now.

Scoring factors (max 100):
  Activity status   0-35  (CQ=35, finishing QSO=25, idle but recent=15, in QSO=5)
  Reverse path      0-25  (PSKReporter SNR — how well they hear you)
  Forward path      0-20  (WSJT-X SNR — how well you hear them)
  Mutual confirm    0-10  (both directions confirmed)
  Novelty           0-10  (never worked > new band > new mode > already worked)
  Time penalty      0 to -15  (stations in unfavorable local time get penalized)
"""

import re
import time
from datetime import datetime, timezone
from typing import Optional


# ── Country → approximate UTC offset ─────────────────────────────── #
# Used to estimate local time at the remote station. Doesn't need to
# be perfect — just close enough to know if it's 3am vs 7pm there.

_COUNTRY_UTC_OFFSET: dict[str, float] = {
    # North America
    "USA": -5, "Canada": -5, "Alaska": -9, "Hawaii": -10,
    "Puerto Rico": -4, "US Virgin Is.": -4, "Mexico": -6,
    # Central America & Caribbean
    "Cuba": -5, "Jamaica": -5, "Dominican Rep.": -4,
    "Costa Rica": -6, "Panama": -5, "Guatemala": -6,
    "Honduras": -6, "El Salvador": -6, "Nicaragua": -6,
    "Belize": -6, "Trinidad": -4, "Barbados": -4,
    # South America
    "Brazil": -3, "Argentina": -3, "Chile": -4, "Colombia": -5,
    "Venezuela": -4, "Peru": -5, "Ecuador": -5, "Uruguay": -3,
    "Paraguay": -4, "Bolivia": -4, "Guyana": -4, "Suriname": -3,
    # Europe
    "England": 0, "Scotland": 0, "Wales": 0, "N. Ireland": 0,
    "Ireland": 0, "France": 1, "Germany": 1, "Spain": 1,
    "Portugal": 0, "Italy": 1, "Netherlands": 1, "Belgium": 1,
    "Switzerland": 1, "Austria": 1, "Denmark": 1, "Norway": 1,
    "Sweden": 1, "Finland": 2, "Poland": 1, "Czech Rep.": 1,
    "Slovakia": 1, "Hungary": 1, "Romania": 2, "Bulgaria": 2,
    "Greece": 2, "Croatia": 1, "Serbia": 1, "Slovenia": 1,
    "Luxembourg": 1, "Iceland": 0, "Estonia": 2, "Latvia": 2,
    "Lithuania": 2, "Ukraine": 2, "Belarus": 3, "Moldova": 2,
    # Russia & Central Asia
    "Russia (European)": 3, "Russia (Asiatic)": 7,
    "Kazakhstan": 6, "Uzbekistan": 5, "Turkmenistan": 5,
    "Kyrgyzstan": 6, "Tajikistan": 5, "Georgia": 4,
    "Armenia": 4, "Azerbaijan": 4,
    # Middle East
    "Israel": 2, "Turkey": 3, "Saudi Arabia": 3, "Iraq": 3,
    "Iran": 3.5, "Kuwait": 3, "UAE": 4, "Qatar": 3,
    "Oman": 4, "Bahrain": 3, "Jordan": 2, "Lebanon": 2,
    "Syria": 2, "Yemen": 3, "Cyprus": 2,
    # Africa
    "South Africa": 2, "Egypt": 2, "Nigeria": 1, "Kenya": 3,
    "Morocco": 1, "Algeria": 1, "Tunisia": 1, "Libya": 2,
    "Ghana": 0, "Senegal": 0, "Cameroon": 1, "Ethiopia": 3,
    "Tanzania": 3, "Uganda": 3, "Zimbabwe": 2, "Mozambique": 2,
    "Namibia": 2, "Botswana": 2, "Madagascar": 3,
    # South Asia
    "India": 5.5, "Pakistan": 5, "Sri Lanka": 5.5,
    "Bangladesh": 6, "Nepal": 5.75,
    # East Asia
    "Japan": 9, "South Korea": 9, "China": 8, "Taiwan": 8,
    "Hong Kong": 8, "Mongolia": 8, "North Korea": 9,
    # Southeast Asia
    "Thailand": 7, "Vietnam": 7, "Philippines": 8,
    "Malaysia": 8, "Singapore": 8, "Indonesia": 7,
    "Myanmar": 6.5, "Cambodia": 7, "Laos": 7,
    # Oceania
    "Australia": 10, "New Zealand": 12, "Papua New Guinea": 10,
    "Fiji": 12, "Guam": 10, "Samoa": 13,
}


def _remote_local_hour(country: str, my_utc_offset: float = -5.0) -> float:
    """Estimate the current local hour (0-23) at a remote station.

    Returns -1 if country is unknown.
    """
    offset = _COUNTRY_UTC_OFFSET.get(country)
    if offset is None:
        return -1.0
    from datetime import datetime, timezone, timedelta
    utc_now = datetime.now(timezone.utc)
    remote_hour = (utc_now + timedelta(hours=offset)).hour
    remote_minute = (utc_now + timedelta(hours=offset)).minute
    return remote_hour + remote_minute / 60.0


def _time_penalty(remote_hour: float) -> tuple[int, str]:
    """Return a penalty (negative points) based on remote local time.

    Ham radio activity peaks 6am-11pm local time. Stations in the
    middle of the night are much less likely to respond.

    Returns (penalty, description).
    """
    if remote_hour < 0:
        return 0, "Unknown timezone"

    h = remote_hour
    if 7 <= h <= 22:
        return 0, f"Local {int(h):02d}:{int((h%1)*60):02d} — prime time"
    if 6 <= h < 7 or 22 < h <= 23:
        return -3, f"Local {int(h):02d}:{int((h%1)*60):02d} — early/late"
    if 5 <= h < 6 or 23 < h <= 24:
        return -7, f"Local {int(h):02d}:{int((h%1)*60):02d} — unlikely hours"
    # 0-5 AM
    return -12, f"Local {int(h):02d}:{int((h%1)*60):02d} — middle of night"


# ── FT8 message patterns ────────────────────────────────────────── #

_CS = r'[A-Z0-9]{1,3}[0-9][A-Z0-9]{0,3}[A-Z](?:/[A-Z0-9]+)?'
_GRID = r'[A-R]{2}[0-9]{2}(?:[a-x]{2})?'

# CQ patterns
_RE_CQ = re.compile(
    rf'^CQ\s+(?:(?P<directed>[A-Z]{{2,4}})\s+)?(?P<caller>{_CS})\s*(?P<grid>{_GRID})?\s*$',
    re.IGNORECASE,
)

# Standard exchange: CALLER TARGET REPORT (e.g., "K1ABC K2XYZ -12")
_RE_REPORT = re.compile(
    rf'^(?P<from>{_CS})\s+(?P<to>{_CS})\s+(?P<report>[R]?[+-]?\d{{1,2}})$',
    re.IGNORECASE,
)

# RR73 / RRR (e.g., "K1ABC K2XYZ RR73")
_RE_RR73 = re.compile(
    rf'^(?P<from>{_CS})\s+(?P<to>{_CS})\s+(?P<end>RR73|RRR)$',
    re.IGNORECASE,
)

# 73 (e.g., "K1ABC K2XYZ 73")
_RE_73 = re.compile(
    rf'^(?P<from>{_CS})\s+(?P<to>{_CS})\s+73$',
    re.IGNORECASE,
)

# Grid-only response (e.g., "K1ABC K2XYZ FN31")
_RE_GRID_RESP = re.compile(
    rf'^(?P<from>{_CS})\s+(?P<to>{_CS})\s+(?P<grid>{_GRID})$',
    re.IGNORECASE,
)


# ── Activity states ─────────────────────────────────────────────── #

STATE_IDLE = "IDLE"
STATE_CQ = "CQ"
STATE_IN_QSO = "IN_QSO"
STATE_FINISHING = "FINISHING"
STATE_CALLING_ME = "CALLING ME"       # They sent a message directed at my callsign
STATE_QSO_WITH_ME = "QSO WITH ME"    # Active QSO exchange with my station

# How long before state reverts to IDLE (seconds)
ACTIVITY_TIMEOUT = 60.0


# ── Confidence labels ────────────────────────────────────────────── #

def _confidence_label(score: int) -> str:
    if score >= 80:
        return "HIGH"
    if score >= 60:
        return "GOOD"
    if score >= 40:
        return "MODERATE"
    if score >= 20:
        return "LOW"
    return "UNLIKELY"


def _recommendation(score: int, state: str, has_mutual: bool) -> str:
    if state == STATE_CALLING_ME:
        return ">>> RESPONDING to your CQ! <<<"
    if state == STATE_QSO_WITH_ME:
        return ">>> ACTIVE QSO with you! <<<"
    if score >= 80:
        if state == STATE_CQ:
            return "Call now — CQing, strong mutual path"
        return "Call now — excellent conditions"
    if score >= 60:
        if has_mutual:
            return "Good candidate — mutual reception confirmed"
        return "Good candidate — they hear you well"
    if score >= 40:
        if state == STATE_FINISHING:
            return "Finishing QSO — may CQ soon"
        return "Worth trying — one-way path confirmed"
    if score >= 20:
        return "Marginal — wait for better conditions"
    return "Unlikely to respond"


# ── FT8 Message Parser ──────────────────────────────────────────── #

def parse_ft8_message(message: str) -> dict:
    """Parse a decoded FT8/FT4 message and extract structured info.

    Returns dict with:
        is_cq       bool    Message is a CQ call
        cq_caller   str     Callsign calling CQ (if is_cq)
        cq_directed str     Directed CQ modifier (e.g., "DX", "NA")
        grid        str     Grid square if present
        is_report   bool    Standard signal report exchange
        is_r_report bool    R+report (response to report)
        is_rr73     bool    RR73/RRR (confirming QSO)
        is_73       bool    73 (final goodbye)
        from_call   str     Sending station callsign
        to_call     str     Receiving station callsign
        report_db   int     Signal report value (if is_report)
    """
    result = {
        "is_cq": False, "cq_caller": "", "cq_directed": "", "grid": "",
        "is_report": False, "is_r_report": False, "is_rr73": False,
        "is_73": False, "from_call": "", "to_call": "", "report_db": None,
    }

    msg = message.strip().upper()
    if not msg:
        return result

    # Check CQ first
    m = _RE_CQ.match(msg)
    if m:
        result["is_cq"] = True
        result["cq_caller"] = m.group("caller").upper()
        result["cq_directed"] = (m.group("directed") or "").upper()
        result["grid"] = (m.group("grid") or "").upper()
        result["from_call"] = result["cq_caller"]
        return result

    # RR73 / RRR
    m = _RE_RR73.match(msg)
    if m:
        result["is_rr73"] = True
        result["from_call"] = m.group("from").upper()
        result["to_call"] = m.group("to").upper()
        return result

    # 73
    m = _RE_73.match(msg)
    if m:
        result["is_73"] = True
        result["from_call"] = m.group("from").upper()
        result["to_call"] = m.group("to").upper()
        return result

    # Signal report (with or without R prefix)
    m = _RE_REPORT.match(msg)
    if m:
        report_str = m.group("report")
        has_r = report_str.upper().startswith("R")
        try:
            db_val = int(report_str.lstrip("Rr"))
        except ValueError:
            db_val = None
        result["is_report"] = True
        result["is_r_report"] = has_r
        result["from_call"] = m.group("from").upper()
        result["to_call"] = m.group("to").upper()
        result["report_db"] = db_val
        return result

    # Grid response (e.g., "K1ABC K2XYZ FN31")
    m = _RE_GRID_RESP.match(msg)
    if m:
        result["is_report"] = True  # Treat as exchange phase
        result["from_call"] = m.group("from").upper()
        result["to_call"] = m.group("to").upper()
        result["grid"] = m.group("grid").upper()
        return result

    return result


# ── Contact Predictor ────────────────────────────────────────────── #

class ContactPredictor:
    """Tracks per-station activity and computes contact probability scores."""

    def __init__(self, my_callsign: str):
        self.my_call = my_callsign.upper().strip()
        # Per-station activity tracking
        # activity[callsign] = {state, directed_to, grid, last_message,
        #                       last_update, snr}
        self.activity: dict[str, dict] = {}

    def update_from_decode(self, raw_message: str, snr: int,
                           timestamp: Optional[float] = None):
        """Process a decoded FT8 message and update activity states.

        Call this for every decode received from WSJT-X/JTDX.
        """
        ts = timestamp or time.time()
        parsed = parse_ft8_message(raw_message)

        if parsed["is_cq"]:
            cs = parsed["cq_caller"]
            if cs and cs != self.my_call:
                self.activity[cs] = {
                    "state": STATE_CQ,
                    "directed_to": "",
                    "grid": parsed["grid"],
                    "last_message": raw_message.strip(),
                    "last_update": ts,
                    "snr": snr,
                }
            return

        from_call = parsed["from_call"]
        to_call = parsed["to_call"]

        if not from_call:
            return

        if parsed["is_73"] or parsed["is_rr73"]:
            # Station is finishing a QSO
            if from_call != self.my_call:
                # If they sent 73/RR73 TO me, QSO completing with me
                if to_call == self.my_call:
                    self._set_state(from_call, STATE_QSO_WITH_ME, self.my_call,
                                    raw_message, ts, snr)
                else:
                    self._set_state(from_call, STATE_FINISHING, to_call,
                                    raw_message, ts, snr)
            if to_call and to_call != self.my_call:
                if from_call == self.my_call:
                    self._set_state(to_call, STATE_QSO_WITH_ME, self.my_call,
                                    raw_message, ts, None)
                else:
                    self._set_state(to_call, STATE_FINISHING, from_call,
                                    raw_message, ts, snr)
            return

        if parsed["is_report"]:
            # Check if this exchange involves MY station
            if to_call == self.my_call and from_call != self.my_call:
                # They are sending a report TO me — active QSO with me
                self._set_state(from_call, STATE_QSO_WITH_ME, self.my_call,
                                raw_message, ts, snr)
                return
            if from_call == self.my_call and to_call and to_call != self.my_call:
                # I am sending a report to them — active QSO with me
                self._set_state(to_call, STATE_QSO_WITH_ME, self.my_call,
                                raw_message, ts, None)
                return

            # Station is in a QSO with someone else
            if from_call != self.my_call:
                self._set_state(from_call, STATE_IN_QSO, to_call,
                                raw_message, ts, snr)
            if to_call and to_call != self.my_call:
                cur = self.activity.get(to_call, {})
                # Don't downgrade from CQ if they just started responding
                if cur.get("state") != STATE_CQ:
                    self._set_state(to_call, STATE_IN_QSO, from_call,
                                    raw_message, ts, None)
            return

        # Check for any message directed at me (e.g., "NC4MH K1ABC FN31"
        # or any unclassified message where to_call is my station)
        if to_call == self.my_call and from_call and from_call != self.my_call:
            cur = self.activity.get(from_call, {})
            # If they were CQing and now calling me, upgrade to CALLING_ME
            if cur.get("state") in (STATE_CQ, STATE_IDLE, None):
                self._set_state(from_call, STATE_CALLING_ME, self.my_call,
                                raw_message, ts, snr)
            else:
                self._set_state(from_call, STATE_QSO_WITH_ME, self.my_call,
                                raw_message, ts, snr)
            return

        # Generic decode — mark as active/idle
        if from_call != self.my_call:
            cur = self.activity.get(from_call, {})
            if cur.get("state") not in (STATE_CQ, STATE_IN_QSO,
                                        STATE_FINISHING, STATE_CALLING_ME,
                                        STATE_QSO_WITH_ME):
                self._set_state(from_call, STATE_IDLE, "",
                                raw_message, ts, snr)

    def _set_state(self, callsign: str, state: str, partner: str,
                   message: str, ts: float, snr: Optional[int]):
        entry = self.activity.get(callsign, {})
        entry["state"] = state
        entry["directed_to"] = partner
        entry["last_message"] = message.strip()
        entry["last_update"] = ts
        if snr is not None:
            entry["snr"] = snr
        # Preserve grid if we had one
        if "grid" not in entry:
            entry["grid"] = ""
        self.activity[callsign] = entry

    def expire_activity(self, max_age: float = ACTIVITY_TIMEOUT):
        """Revert stations to IDLE if no activity within max_age seconds."""
        now = time.time()
        expired = []
        for cs, info in self.activity.items():
            if now - info.get("last_update", 0) > max_age:
                expired.append(cs)
        for cs in expired:
            del self.activity[cs]

    def compute_score(
        self,
        callsign: str,
        heard_snr: Optional[int],
        heard_age_s: float,
        spot_snr: Optional[int],
        spot_age_s: float,
        is_worked_band_mode: bool,
        is_worked_band: bool,
        is_worked_any: bool,
        country: str = "",
    ) -> dict:
        """Compute contact probability score for a single station.

        Parameters
        ----------
        callsign : target station callsign
        heard_snr : SNR you decoded them at (None if not heard)
        heard_age_s : seconds since last decode
        spot_snr : SNR they reported hearing you at (None if no spot)
        spot_age_s : seconds since last PSKReporter spot
        is_worked_band_mode : already worked on this band+mode
        is_worked_band : already worked on this band (any mode)
        is_worked_any : already worked on any band/mode
        country : DXCC country name for time zone estimation

        Returns dict with score, confidence, factors, recommendation.
        """
        factors = {}
        total = 0

        # ── 1. Activity Status (0-35, or 99 for active connection) ─
        act = self.activity.get(callsign, {})
        state = act.get("state", STATE_IDLE)
        last_update = act.get("last_update", 0)
        activity_age = time.time() - last_update if last_update else 999

        # Active connection with MY station — override to 99%
        if state in (STATE_CALLING_ME, STATE_QSO_WITH_ME):
            partner = act.get("directed_to", "")
            if state == STATE_CALLING_ME:
                label = "RESPONDING to your CQ!"
            else:
                label = "ACTIVE QSO with you!"
            factors["activity"] = label
            factors["reverse_path"] = "N/A — active connection"
            factors["forward_path"] = "N/A — active connection"
            factors["mutual"] = "Confirmed — direct exchange"
            factors["novelty"] = "N/A"
            conf = "HIGH"
            rec = f">>> {label} <<<"
            return {
                "callsign": callsign,
                "score": 99,
                "confidence": conf,
                "factors": factors,
                "recommendation": rec,
                "state": state,
                "grid": act.get("grid", ""),
            }

        if state == STATE_CQ and activity_age < 30:
            pts = 35
            factors["activity"] = f"CQ ({activity_age:.0f}s ago)"
        elif state == STATE_CQ:
            pts = 28
            factors["activity"] = f"CQ ({activity_age:.0f}s ago, aging)"
        elif state == STATE_FINISHING and activity_age < 30:
            pts = 25
            factors["activity"] = "Finishing QSO — may CQ next"
        elif state == STATE_FINISHING:
            pts = 18
            factors["activity"] = f"Finished QSO {activity_age:.0f}s ago"
        elif state == STATE_IN_QSO:
            partner = act.get("directed_to", "")
            pts = 5
            factors["activity"] = f"In QSO with {partner or '?'}"
        elif activity_age < 60:
            pts = 15
            factors["activity"] = f"Active {activity_age:.0f}s ago"
        else:
            pts = 0
            factors["activity"] = "No recent activity"
        total += pts

        # ── 2. Reverse Path — They Hear You (0-25) ──────────────
        if spot_snr is not None:
            if spot_snr >= 0:
                pts = 25
            elif spot_snr >= -5:
                pts = 20
            elif spot_snr >= -10:
                pts = 15
            elif spot_snr >= -15:
                pts = 10
            elif spot_snr >= -20:
                pts = 5
            else:
                pts = 2
            # Freshness bonus
            if spot_age_s < 60:
                pts = min(pts + 5, 30)
            factors["reverse_path"] = f"They hear you at {spot_snr:+d} dB ({spot_age_s:.0f}s ago)"
        else:
            pts = 0
            factors["reverse_path"] = "No PSKReporter data"
        total += pts

        # ── 3. Forward Path — You Hear Them (0-20) ──────────────
        if heard_snr is not None:
            if heard_snr >= 0:
                pts = 20
            elif heard_snr >= -5:
                pts = 17
            elif heard_snr >= -10:
                pts = 14
            elif heard_snr >= -15:
                pts = 10
            elif heard_snr >= -20:
                pts = 5
            else:
                pts = 2
            # Freshness bonus
            if heard_age_s < 30:
                pts = min(pts + 3, 23)
            factors["forward_path"] = f"You hear them at {heard_snr:+d} dB ({heard_age_s:.0f}s ago)"
        else:
            pts = 0
            factors["forward_path"] = "Not currently heard"
        total += pts

        # ── 4. Mutual Confirmation (0-10) ────────────────────────
        has_mutual = (heard_snr is not None and spot_snr is not None)
        if has_mutual:
            pts = 10
            factors["mutual"] = "Both directions confirmed"
        elif heard_snr is not None or spot_snr is not None:
            pts = 3
            factors["mutual"] = "One-way path only"
        else:
            pts = 0
            factors["mutual"] = "No path data"
        total += pts

        # ── 5. Novelty (0-10) ────────────────────────────────────
        if not is_worked_any:
            pts = 10
            factors["novelty"] = "New station — never worked"
        elif not is_worked_band:
            pts = 7
            factors["novelty"] = "New band for this station"
        elif not is_worked_band_mode:
            pts = 5
            factors["novelty"] = "New mode for this station"
        else:
            pts = 0
            factors["novelty"] = "Already worked (band+mode)"
        total += pts

        # ── Build result ─────────────────────────────────────────
        conf = _confidence_label(total)
        rec = _recommendation(total, state, has_mutual)

        return {
            "callsign": callsign,
            "score": total,
            "confidence": conf,
            "factors": factors,
            "recommendation": rec,
            "state": state,
            "grid": act.get("grid", ""),
        }

    def rank_stations(
        self,
        heard: dict,
        spotted_by: dict,
        logged: dict,
        band: str,
        mode: str,
        top_n: int = 20,
        country_lookup: Optional[callable] = None,
    ) -> list[dict]:
        """Rank all known stations by contact probability.

        Parameters
        ----------
        heard : {callsign: {snr, mode, time}} from AppState
        spotted_by : {callsign: {snr, band, mode, time}} from AppState
        logged : {(call, band, mode): date} from AppState
        band : current operating band (e.g., "20m")
        mode : current operating mode (e.g., "FT8")
        top_n : max stations to return
        country_lookup : callable(callsign) -> str, returns DXCC country

        Returns sorted list of score dicts, highest first.
        """
        now = time.time()
        candidates: set[str] = set()

        # All stations we know about
        candidates.update(heard.keys())
        candidates.update(spotted_by.keys())
        candidates.update(self.activity.keys())

        # Remove ourselves
        candidates.discard(self.my_call)

        results = []
        for cs in candidates:
            h = heard.get(cs, {})
            s = spotted_by.get(cs, {})

            # Heard info
            heard_snr = h.get("snr") if h else None
            h_time = h.get("time")
            if h_time:
                if isinstance(h_time, datetime):
                    heard_age = (datetime.now(timezone.utc) - h_time).total_seconds()
                else:
                    heard_age = now - h_time
            else:
                heard_age = 9999

            # Spot info
            spot_snr = s.get("snr") if s else None
            s_time = s.get("time")
            if s_time:
                if isinstance(s_time, (int, float)):
                    spot_age = now - s_time
                else:
                    spot_age = 9999
            else:
                spot_age = 9999

            # Worked status
            band_l = band.lower() if band else ""
            mode_u = mode.upper() if mode else ""
            is_worked_bm = (cs, band_l, mode_u) in logged
            is_worked_b = is_worked_bm or (cs, band_l, "") in logged
            is_worked_any = is_worked_b or (cs, "", mode_u) in logged or (cs, "", "") in logged

            # Get country for time zone penalty
            country = country_lookup(cs) if country_lookup else ""

            score = self.compute_score(
                cs,
                heard_snr=heard_snr,
                heard_age_s=heard_age,
                spot_snr=spot_snr,
                spot_age_s=spot_age,
                is_worked_band_mode=is_worked_bm,
                is_worked_band=is_worked_b,
                is_worked_any=is_worked_any,
                country=country,
            )

            # Only include stations with some signal of viability
            if score["score"] > 0:
                # Add extra display info
                score["heard_snr"] = heard_snr
                score["spot_snr"] = spot_snr
                score["heard_age"] = heard_age
                score["spot_age"] = spot_age
                results.append(score)

        # Sort by score descending
        results.sort(key=lambda r: r["score"], reverse=True)
        return results[:top_n]
