#!/usr/bin/env python3
"""
Sends ONE FCM notification per kick-off time, LEAD_SECONDS (default 120s)
before the matches start. All matches starting at the same minute are grouped.

Data source : events.json on GitHub (EVENTS_URL)  -> no Firebase database needed
State       : state/sent_notifications.json (committed by the workflow) -> dedupe
Delivery    : FCM HTTP v1 via firebase-admin (topic message)

Only ONE secret is required: FIREBASE_SERVICE_ACCOUNT (service-account JSON).
"""
import base64
import json
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests


# ----------------------------------------------------------------- config ---
def env(name, default=""):
    """Env var that treats an EMPTY value (e.g. an unset GitHub secret) as missing."""
    val = (os.environ.get(name) or "").strip()
    return val if val else default


EVENTS_URL = env(
    "EVENTS_URL",
    "https://raw.githubusercontent.com/sarsx3/aladin450/refs/heads/main/events.json",
)
TOPIC = env("FCM_TOPIC", "match_alerts")
CHANNEL_ID = env("ANDROID_CHANNEL_ID", "match_alerts")
SOURCE_UTC_OFFSET = float(env("SOURCE_UTC_OFFSET_HOURS", "6"))  # events.json times = Bangladesh time
LEAD_SECONDS = int(env("LEAD_SECONDS", "120"))
LOOKAHEAD_SECONDS = int(env("LOOKAHEAD_SECONDS", "330"))
MAX_LINES = int(env("MAX_LINES", "6"))
DRY_RUN = env("DRY_RUN", "0") == "1"
TEST_PUSH = env("TEST_PUSH", "0") == "1"
STATE_PATH = Path(env("STATE_PATH", "state/sent_notifications.json"))
STATE_KEEP_DAYS = 3

PLACEHOLDER_TEAMS = {"", "tbc", "tbd", "-", "n/a", "unknown"}

SPORT_EMOJI = {
    "football": "⚽", "cricket": "🏏", "baseball": "⚾", "motorsport": "🏁",
    "basketball": "🏀", "tennis": "🎾", "hockey": "🏒", "rugby": "🏉",
    "mma": "🥊", "boxing": "🥊", "volleyball": "🏐",
}

LAST_SOURCE_META = {}  # filled by load_events() (used by the preview)


def log(msg):
    print(msg, flush=True)


def utcnow():
    """Current UTC time. FAKE_NOW (ISO string) is only for local testing."""
    fake = env("FAKE_NOW")
    if fake:
        return datetime.fromisoformat(fake.replace("Z", "+00:00")).astimezone(timezone.utc)
    return datetime.now(timezone.utc)


# ------------------------------------------------------------ events.json ---
def load_events():
    """Download events.json (with retries + cache-busting) and return a list of dicts."""
    last_exc = None
    data = None
    for attempt in range(1, 5):
        try:
            r = requests.get(
                EVENTS_URL,
                params={"_": int(time.time())},  # raw.githubusercontent caches ~5 min
                headers={"Cache-Control": "no-cache", "User-Agent": "fcm-match-reminders/2.0"},
                timeout=25,
            )
            r.raise_for_status()
            data = json.loads(r.content.decode("utf-8-sig"))
            break
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            log(f"events.json download failed (try {attempt}/4): {exc}")
            time.sleep(2 * attempt)
    if data is None:
        raise RuntimeError(f"Could not load events from {EVENTS_URL}: {last_exc}")

    if isinstance(data, dict):
        LAST_SOURCE_META.update(
            {
                "last_updated": data.get("last_updated"),
                "last_updated_iso": data.get("last_updated_iso"),
                "total_events": data.get("total_events"),
            }
        )
        items = data.get("events")
        if items is None:  # unknown wrapper -> accept {id: event, ...}
            items = list(data.values())
    else:
        items = data
    if isinstance(items, dict):
        items = list(items.values())
    return [e for e in (items or []) if isinstance(e, dict)]


# ------------------------------------------------------------------ state ---
def load_state():
    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def save_state(state):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(STATE_PATH)


def prune_state(state, now):
    cutoff = (now - timedelta(days=STATE_KEEP_DAYS)).strftime("%Y%m%d%H%M")
    for k in [k for k in state if k < cutoff]:
        del state[k]


# ---------------------------------------------------------------- parsing ---
_DATE_FORMATS = ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d")
_TIME_FORMATS = ("%I:%M %p", "%I:%M%p", "%H:%M")


def parse_start(ev):
    """'14/10/2026' + '10:45 PM' (Bangladesh time) -> aware UTC datetime."""
    d = str(ev.get("date") or ev.get("matchDate") or "").strip()
    t = str(ev.get("time") or ev.get("matchTime") or "").replace("\u202f", " ").replace("\xa0", " ")
    t = re.sub(r"\s+", " ", t).strip().upper()
    for df in _DATE_FORMATS:
        for tf in _TIME_FORMATS:
            try:
                local = datetime.strptime(f"{d} {t}", f"{df} {tf}")
            except ValueError:
                continue
            return (local - timedelta(hours=SOURCE_UTC_OFFSET)).replace(tzinfo=timezone.utc)
    return None


def sport_of(ev):
    lg = str(ev.get("league") or "")
    s = ev.get("sportCategory") or (lg.split("|", 1)[0] if "|" in lg else "")
    return str(s).strip()


def league_of(ev):
    lg = str(ev.get("league") or "")
    return lg.split("|", 1)[1].strip() if "|" in lg else lg.strip()


def team(ev, n):
    return str(ev.get(f"team{n}") or "").strip()


def is_versus(ev):
    t1, t2 = team(ev, 1), team(ev, 2)
    if t1.lower() in PLACEHOLDER_TEAMS or t2.lower() in PLACEHOLDER_TEAMS:
        return False
    return t1.lower() != t2.lower()


def is_hot(ev):
    return (
        ev.get("isHot") is True
        or ev.get("is_hot") is True
        or str(ev.get("isHot", "")).lower() in ("true", "yes", "1")
        or str(ev.get("hotMatch", "")).lower() == "yes"
    )


def is_public(ev):
    return str(ev.get("visibility") or "public").strip().lower() == "public"


def emoji_of(ev):
    return SPORT_EMOJI.get(sport_of(ev).lower(), "🏆")


def sort_key(ev):
    return (not is_hot(ev), league_of(ev), team(ev, 1))


# ---------------------------------------------------------------- message ---
def lead_phrase(seconds_left):
    if seconds_left <= 45:
        return "starting now"
    m = max(1, round(seconds_left / 60))
    return f"starting in {m} minute{'s' if m > 1 else ''}"


def line_for(ev):
    icon = "🔥" if is_hot(ev) else emoji_of(ev)
    if is_versus(ev):
        return f"{icon} {team(ev, 1)} vs {team(ev, 2)} · {league_of(ev)}"
    return f"{icon} {league_of(ev)}"


def build_text(evs, seconds_left):
    phrase = lead_phrase(seconds_left)
    evs = sorted(evs, key=sort_key)
    n = len(evs)

    if n == 1:
        ev = evs[0]
        icon = "🔥" if is_hot(ev) else emoji_of(ev)
        if is_versus(ev):
            title = f"{icon} {team(ev, 1)} vs {team(ev, 2)}"
            body = f"{league_of(ev)} · {phrase.capitalize()}"
        else:
            title = f"{icon} {league_of(ev)}"
            body = phrase.capitalize()
        return title, body

    title = f"⏰ {n} matches {phrase}"
    lines = [line_for(e) for e in evs[:MAX_LINES]]
    if n > MAX_LINES:
        extra = n - MAX_LINES
        lines.append(f"+ {extra} more match{'es' if extra > 1 else ''} in the app")
    return title, "\n".join(lines)


# -------------------------------------------------------------------- FCM ---
def init_firebase():
    import firebase_admin
    from firebase_admin import credentials

    raw = env("FIREBASE_SERVICE_ACCOUNT")
    if not raw:
        raise RuntimeError("Secret FIREBASE_SERVICE_ACCOUNT is empty/missing (add the service-account JSON).")
    if not raw.lstrip().startswith("{"):  # allow base64-encoded JSON too
        raw = base64.b64decode(raw).decode("utf-8")
    info = json.loads(raw, strict=False)
    if isinstance(info.get("private_key"), str):
        info["private_key"] = info["private_key"].replace("\\n", "\n")
    firebase_admin.initialize_app(credentials.Certificate(info))


def dispatch(message_kwargs, title, body):
    """Build + send (or print in dry-run) one FCM topic message."""
    from firebase_admin import messaging

    if DRY_RUN:
        log(f"[DRY RUN] topic={TOPIC}\n  title: {title}\n  body :\n    " + body.replace("\n", "\n    "))
        return "dry-run"
    return messaging.send(messaging.Message(**message_kwargs))


def send_fcm(title, body, evs, start, seconds_left):
    from firebase_admin import messaging

    ids = [str(e.get("id", "")) for e in evs if e.get("id")]
    ttl = timedelta(seconds=max(60, int(seconds_left) + 120))  # drop it if it would arrive stale
    key = start.strftime("%Y%m%d%H%M")

    kwargs = dict(
        topic=TOPIC,
        notification=messaging.Notification(title=title, body=body),
        data={
            "type": "match_reminder",
            "start_time_utc": start.isoformat(),
            "match_count": str(len(evs)),
            "match_ids": ",".join(ids),
            "first_match_id": ids[0] if ids else "",
        },
        android=messaging.AndroidConfig(
            priority="high",
            ttl=ttl,
            collapse_key="match_reminder",
            notification=messaging.AndroidNotification(
                channel_id=CHANNEL_ID,
                tag=f"match_{key}",
                sound="default",
                default_vibrate_timings=True,
                priority="high",
                visibility="public",
            ),
        ),
        apns=messaging.APNSConfig(
            headers={
                "apns-priority": "10",
                "apns-expiration": str(int(time.time() + ttl.total_seconds())),
            },
            payload=messaging.APNSPayload(
                aps=messaging.Aps(sound="default", thread_id="match_reminder")
            ),
        ),
    )
    return dispatch(kwargs, title, body)


def send_test():
    from firebase_admin import messaging

    title, body = "✅ FCM test", "GitHub Action → Firebase → your phone works."
    kwargs = dict(
        topic=TOPIC,
        notification=messaging.Notification(title=title, body=body),
        data={"type": "test"},
        android=messaging.AndroidConfig(
            priority="high",
            notification=messaging.AndroidNotification(channel_id=CHANNEL_ID, sound="default"),
        ),
    )
    return dispatch(kwargs, title, body)


# ------------------------------------------------------------------- main ---
def group_events(events, now=None):
    """-> {start_utc: [events]} for public events with a valid time (and start > now, if given)."""
    groups = defaultdict(list)
    skipped = 0
    for ev in events:
        if not is_public(ev):
            continue
        start = parse_start(ev)
        if not start:
            skipped += 1
            continue
        if now is None or start > now:
            groups[start].append(ev)
    if skipped:
        log(f"Warning: {skipped} event(s) skipped (unreadable date/time)")
    return groups


def main():
    if TEST_PUSH:
        log(f"Sending TEST notification to topic '{TOPIC}'")
        if not DRY_RUN:
            init_firebase()
        log(f"test -> {send_test()}")
        return 0

    now = utcnow()
    events = load_events()
    log(f"Loaded {len(events)} events from {EVENTS_URL}")

    state = load_state()
    prune_state(state, now)

    due = []
    for start, evs in group_events(events, now).items():
        send_at = start - timedelta(seconds=LEAD_SECONDS)
        if send_at > now + timedelta(seconds=LOOKAHEAD_SECONDS):
            continue  # a later run will handle it
        due.append((send_at, start, evs))
    due.sort(key=lambda x: x[0])

    if not due:
        log("Nothing due in this window.")
        if not DRY_RUN:
            save_state(state)  # persists pruning
        return 0

    if not DRY_RUN:
        init_firebase()

    failures = 0
    for send_at, start, evs in due:
        key = start.strftime("%Y%m%d%H%M")
        if key in state:
            log(f"[{key}] already sent, skipping")
            continue

        wait = (send_at - utcnow()).total_seconds()
        if wait > 0 and not env("FAKE_NOW"):
            log(f"[{key}] waiting {wait:.0f}s ({len(evs)} match(es))")
            time.sleep(wait)

        seconds_left = (start - utcnow()).total_seconds()
        if env("FAKE_NOW"):
            seconds_left = float(LEAD_SECONDS)
        if seconds_left <= 0:
            log(f"[{key}] missed (already started)")
            continue

        title, body = build_text(evs, seconds_left)
        try:
            msg_id = send_fcm(title, body, evs, start, seconds_left)
            log(f"[{key}] sent: {title} -> {msg_id}")
            if not DRY_RUN:
                state[key] = {"at": utcnow().isoformat(timespec="seconds"), "count": len(evs), "title": title}
                save_state(state)  # saved immediately after every successful send
        except Exception as exc:  # noqa: BLE001
            failures += 1
            log(f"[{key}] FAILED: {exc}")

    if not DRY_RUN:
        save_state(state)
    return 1 if failures else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except RuntimeError as exc:  # clear one-line error instead of a traceback
        log(f"ERROR: {exc}")
        sys.exit(1)
