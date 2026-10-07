#!/usr/bin/env python3
"""
Sends ONE FCM notification per kick-off time, LEAD_SECONDS (default 120s)
before the matches start. All matches starting at the same minute are grouped.

Data source : Firebase Realtime Database  /sports_events.json
State       : Firebase Realtime Database  /notif_sent/<yyyymmddHHMM>   (dedupe)
Delivery    : FCM HTTP v1 via firebase-admin (topic message)
"""
import json
import math
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import requests

# ----------------------------------------------------------------- config ---
DB_BASE = os.environ["FIREBASE_DB_BASE"].rstrip("/")   # https://xxx-default-rtdb.firebaseio.com
DB_SECRET = os.environ["FIREBASE_DB_SECRET"]
TOPIC = os.environ.get("FCM_TOPIC", "match_alerts")
CHANNEL_ID = os.environ.get("ANDROID_CHANNEL_ID", "match_alerts")
SOURCE_UTC_OFFSET = float(os.environ.get("SOURCE_UTC_OFFSET_HOURS", "6"))  # matchTime is Bangladesh time
LEAD_SECONDS = int(os.environ.get("LEAD_SECONDS", "120"))
LOOKAHEAD_SECONDS = int(os.environ.get("LOOKAHEAD_SECONDS", "330"))
MAX_LINES = int(os.environ.get("MAX_LINES", "6"))
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"

SPORT_EMOJI = {
    "football": "⚽", "cricket": "🏏", "baseball": "⚾", "motorsport": "🏁",
    "basketball": "🏀", "tennis": "🎾", "hockey": "🏒", "rugby": "🏉",
    "mma": "🥊", "boxing": "🥊", "volleyball": "🏐",
}


def log(msg):
    print(msg, flush=True)


# ------------------------------------------------------------ RTDB helpers ---
def db_request(method, path, **kw):
    params = {"auth": DB_SECRET, **kw.pop("params", {})}
    r = requests.request(method, f"{DB_BASE}/{path}.json", params=params, timeout=25, **kw)
    r.raise_for_status()
    return r.json() if r.content else None


def load_events():
    data = db_request("GET", "sports_events") or {}
    items = data.values() if isinstance(data, dict) else data
    return [e for e in items if isinstance(e, dict)]


def is_claimed(key):
    return db_request("GET", f"notif_sent/{key}") is not None


def claim(key, payload):
    db_request("PUT", f"notif_sent/{key}", data=json.dumps(payload))


def release(key):
    db_request("DELETE", f"notif_sent/{key}")


def cleanup_old_state(now):
    cutoff = (now - timedelta(days=3)).strftime("%Y%m%d%H%M")
    keys = db_request("GET", "notif_sent", params={"shallow": "true"}) or {}
    for k in keys:
        if k < cutoff:
            db_request("DELETE", f"notif_sent/{k}")


# ---------------------------------------------------------------- parsing ---
def parse_start(ev):
    """'08/10/2026' + '4:00 AM' (Bangladesh time) -> aware UTC datetime."""
    try:
        local = datetime.strptime(f"{ev['matchDate']} {ev['matchTime']}", "%d/%m/%Y %I:%M %p")
    except (KeyError, ValueError, TypeError):
        return None
    return (local - timedelta(hours=SOURCE_UTC_OFFSET)).replace(tzinfo=timezone.utc)


def sport_of(ev):
    s = (ev.get("sportCategory") or ev.get("league", "").split("|")[0]).strip()
    return s


def league_of(ev):
    lg = ev.get("league", "")
    return lg.split("|", 1)[1].strip() if "|" in lg else lg.strip()


def is_versus(ev):
    t1, t2 = (ev.get("team1") or "").strip(), (ev.get("team2") or "").strip()
    return bool(t1 and t2 and t1.lower() != t2.lower())


def is_hot(ev):
    return str(ev.get("hotMatch", "")).lower() == "yes" or ev.get("is_hot") is True


def emoji_of(ev):
    return SPORT_EMOJI.get(sport_of(ev).lower(), "🏆")


# ---------------------------------------------------------------- message ---
def lead_phrase(seconds_left):
    if seconds_left <= 45:
        return "starting now"
    m = max(1, round(seconds_left / 60))
    return f"starting in {m} minute{'s' if m > 1 else ''}"


def line_for(ev):
    icon = "🔥" if is_hot(ev) else emoji_of(ev)
    if is_versus(ev):
        return f"{icon} {ev['team1'].strip()} vs {ev['team2'].strip()} · {league_of(ev)}"
    return f"{icon} {league_of(ev)}"


def build_text(evs, seconds_left):
    phrase = lead_phrase(seconds_left)
    evs = sorted(evs, key=lambda e: (not is_hot(e), league_of(e), e.get("team1", "")))
    n = len(evs)

    if n == 1:
        ev = evs[0]
        title = f"🔥 Big match {phrase}" if is_hot(ev) else f"{emoji_of(ev)} {phrase.capitalize()}"
        if is_versus(ev):
            body = f"{ev['team1'].strip()} vs {ev['team2'].strip()}\n{league_of(ev)}"
        else:
            body = league_of(ev)
        return title, body

    title = f"⏰ {n} matches {phrase}"
    lines = [line_for(e) for e in evs[:MAX_LINES]]
    if n > MAX_LINES:
        extra = n - MAX_LINES
        lines.append(f"+ {extra} more match{'es' if extra > 1 else ''} in the app")
    return title, "\n".join(lines)


def send_fcm(title, body, evs, start, seconds_left):
    from firebase_admin import messaging

    ids = [str(e.get("id", "")) for e in evs if e.get("id")]
    ttl = timedelta(seconds=max(60, int(seconds_left) + 120))  # drop it if it would arrive stale
    key = start.strftime("%Y%m%d%H%M")

    message = messaging.Message(
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
                notification_priority="PRIORITY_HIGH",
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

    if DRY_RUN:
        log(f"[DRY RUN] topic={TOPIC}\n  title: {title}\n  body :\n    " + body.replace("\n", "\n    "))
        return "dry-run"
    return messaging.send(message)


# ------------------------------------------------------------------- main ---
def main():
    now = datetime.now(timezone.utc)
    events = load_events()
    log(f"Loaded {len(events)} events")

    groups = defaultdict(list)
    for ev in events:
        if (ev.get("visibility") or "public") != "public":
            continue
        start = parse_start(ev)
        if start:
            groups[start].append(ev)

    due = []
    for start, evs in groups.items():
        send_at = start - timedelta(seconds=LEAD_SECONDS)
        if start <= now:
            continue  # already started
        if send_at > now + timedelta(seconds=LOOKAHEAD_SECONDS):
            continue  # a later run will handle it
        due.append((send_at, start, evs))
    due.sort(key=lambda x: x[0])

    if not due:
        log("Nothing due in this window.")
        return 0

    if not DRY_RUN:
        import firebase_admin
        from firebase_admin import credentials

        cred = credentials.Certificate(json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"]))
        firebase_admin.initialize_app(cred)

    failures = 0
    for send_at, start, evs in due:
        key = start.strftime("%Y%m%d%H%M")
        if is_claimed(key):
            log(f"[{key}] already sent, skipping")
            continue

        wait = (send_at - datetime.now(timezone.utc)).total_seconds()
        if wait > 0:
            log(f"[{key}] waiting {wait:.0f}s ({len(evs)} match(es))")
            time.sleep(wait)

        seconds_left = (start - datetime.now(timezone.utc)).total_seconds()
        if seconds_left <= 0:
            log(f"[{key}] missed (already started)")
            continue

        title, body = build_text(evs, seconds_left)
        try:
            if not DRY_RUN:
                claim(key, {"at": datetime.now(timezone.utc).isoformat(), "count": len(evs)})
            msg_id = send_fcm(title, body, evs, start, seconds_left)
            log(f"[{key}] sent: {title} -> {msg_id}")
        except Exception as exc:  # noqa: BLE001
            failures += 1
            log(f"[{key}] FAILED: {exc}")
            if not DRY_RUN:
                try:
                    release(key)  # let the next run retry
                except Exception:  # noqa: BLE001
                    pass

    try:
        cleanup_old_state(datetime.now(timezone.utc))
    except Exception as exc:  # noqa: BLE001
        log(f"cleanup skipped: {exc}")

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
