#!/usr/bin/env python3
"""
Builds preview/upcoming_notifications.json  -> exactly what the FCM sender
will push in the future (title, body, matches, send time, status).

* Re-uses the same grouping / text code as send_match_reminders.py, so the
  preview can never drift from the real notifications.
* Writes the file only when something really changed (or every
  PREVIEW_HEARTBEAT_MINUTES as a heartbeat) -> no commit spam.
* Also prints a nice table into the GitHub Actions run summary.
"""
import json
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import send_match_reminders as core  # same folder (scripts/)

PREVIEW_PATH = Path(os.environ.get("PREVIEW_PATH", "preview/upcoming_notifications.json"))
HORIZON_HOURS = int(os.environ.get("PREVIEW_HOURS", "72"))
MAX_GROUPS = int(os.environ.get("PREVIEW_MAX_GROUPS", "100"))
HEARTBEAT_MIN = int(os.environ.get("PREVIEW_HEARTBEAT_MINUTES", "60"))
SYNC_INTERVAL_MIN = int(os.environ.get("SYNC_INTERVAL_MINUTES", "5"))

LOCAL_TZ = timezone(timedelta(hours=core.SOURCE_UTC_OFFSET))
TZ_LABEL = f"UTC{core.SOURCE_UTC_OFFSET:+g} (Bangladesh time)" if core.SOURCE_UTC_OFFSET == 6 \
    else f"UTC{core.SOURCE_UTC_OFFSET:+g}"


def fmt_local(dt):
    return dt.astimezone(LOCAL_TZ).strftime("%Y-%m-%d %I:%M %p")


def match_entry(ev):
    versus = core.is_versus(ev)
    return {
        "id": str(ev.get("id", "")),
        "sport": core.sport_of(ev),
        "league": core.league_of(ev),
        "team1": (ev.get("team1") or "").strip() if versus else None,
        "team2": (ev.get("team2") or "").strip() if versus else None,
        "hot": core.is_hot(ev),
        "display": core.line_for(ev),
    }


def build_payload(now):
    events = core.load_events()
    sent_keys = set((core.db_request("GET", "notif_sent", params={"shallow": "true"}) or {}).keys())

    groups = defaultdict(list)
    for ev in events:
        if (ev.get("visibility") or "public") != "public":
            continue
        start = core.parse_start(ev)
        if start and start > now:
            groups[start].append(ev)

    horizon = now + timedelta(hours=HORIZON_HOURS)
    items = []
    for start in sorted(groups):
        if start > horizon or len(items) >= MAX_GROUPS:
            break
        evs = sorted(
            groups[start],
            key=lambda e: (not core.is_hot(e), core.league_of(e), e.get("team1", "")),
        )
        title, body = core.build_text(evs, core.LEAD_SECONDS)
        key = start.strftime("%Y%m%d%H%M")
        send_at = start - timedelta(seconds=core.LEAD_SECONDS)
        if key in sent_keys:
            status = "sent"
        elif send_at <= now:
            status = "due_now"
        else:
            status = "scheduled"
        items.append(
            {
                "status": status,
                "send_at_local": fmt_local(send_at),
                "match_start_local": fmt_local(start),
                "send_at_utc": send_at.isoformat(timespec="seconds"),
                "match_start_utc": start.isoformat(timespec="seconds"),
                "match_count": len(evs),
                "notification": {"title": title, "body": body},
                "matches": [match_entry(e) for e in evs],
            }
        )

    return {
        "generated_at": now.isoformat(timespec="seconds"),
        "sync": {
            "schedule": f"every {SYNC_INTERVAL_MIN} minutes (GitHub Actions cron)",
            "interval_minutes": SYNC_INTERVAL_MIN,
            "source_events_loaded": len(events),
            "timezone": TZ_LABEL,
            "note": "GitHub may delay scheduled runs by a few minutes at busy times.",
        },
        "settings": {
            "fcm_topic": core.TOPIC,
            "send_before_start_minutes": core.LEAD_SECONDS // 60,
            "preview_horizon_hours": HORIZON_HOURS,
        },
        "summary": {
            "upcoming_notifications": len(items),
            "upcoming_matches": sum(i["match_count"] for i in items),
            "next_notification_at_local": next(
                (i["send_at_local"] for i in items if i["status"] != "sent"), None
            ),
        },
        "upcoming_notifications": items,
    }


def stable(p):
    d = dict(p)
    d.pop("generated_at", None)
    return d


def write_if_needed(payload, now):
    if PREVIEW_PATH.exists():
        try:
            old = json.loads(PREVIEW_PATH.read_text(encoding="utf-8"))
            age = now - datetime.fromisoformat(old["generated_at"])
            if stable(old) == stable(payload) and age < timedelta(minutes=HEARTBEAT_MIN):
                core.log("Preview unchanged -> file not rewritten")
                return False
        except Exception:  # noqa: BLE001
            pass  # broken/old file -> just rewrite
    PREVIEW_PATH.parent.mkdir(parents=True, exist_ok=True)
    PREVIEW_PATH.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    core.log(f"Preview written: {PREVIEW_PATH}")
    return True


def cell(text):
    return str(text).replace("|", "\\|").replace("\n", "<br>")


def write_step_summary(payload):
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    s = payload["sync"]
    lines = [
        "## 🔔 Upcoming FCM notifications",
        "",
        f"**Last sync:** `{payload['generated_at']}` · **Schedule:** {s['schedule']} · "
        f"**Events loaded:** {s['source_events_loaded']} · **Times:** {s['timezone']}",
        "",
        f"**Next notification:** {payload['summary']['next_notification_at_local'] or '—'}  ",
        f"**Upcoming:** {payload['summary']['upcoming_notifications']} notifications "
        f"({payload['summary']['upcoming_matches']} matches)",
        "",
        "| Send at | Status | Notification |",
        "|---|---|---|",
    ]
    icon = {"sent": "✅ sent", "due_now": "⏳ due", "scheduled": "🕒 scheduled"}
    for i in payload["upcoming_notifications"][:30]:
        n = i["notification"]
        lines.append(
            f"| {cell(i['send_at_local'])} | {icon[i['status']]} | "
            f"**{cell(n['title'])}**<br>{cell(n['body'])} |"
        )
    with open(path, "a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def main():
    now = datetime.now(timezone.utc)
    payload = build_payload(now)
    write_if_needed(payload, now)
    write_step_summary(payload)
    core.log(
        f"Preview: {payload['summary']['upcoming_notifications']} upcoming notifications, "
        f"{payload['summary']['upcoming_matches']} matches"
    )


if __name__ == "__main__":
    main()
