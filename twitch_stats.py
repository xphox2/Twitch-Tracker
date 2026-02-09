#!/usr/bin/env python3
"""
Twitch Stats Tracker
Downloads and archives your Twitch stats locally for OBS integration.
Tracks: subscribers, bits, earnings (70%), artist donations (30%)
Includes built-in HTTP server for OBS Browser Source overlay.
"""

import requests
import json
from json import JSONDecodeError
import csv
import os
import time
import threading
import http.server
import socketserver
import glob
from urllib.parse import urlencode, urlparse
from datetime import datetime, timedelta
from pathlib import Path
import socket
import ssl
import re

CONFIG_FILE = "config.json"
DATA_DIR = "data"
ARCHIVE_DIR = os.path.join(DATA_DIR, "archive", "daily")
LATEST_FILE = os.path.join(DATA_DIR, "latest_stats.json")
ARTIST_EARNINGS_FILE = os.path.join(DATA_DIR, "artist_earnings.json")
DAILY_BITS_FILE = os.path.join(DATA_DIR, "daily_bits.json")
BITS_TRACK_FILE = os.path.join(DATA_DIR, "bits_tracker.json")
SUB_TIERS_FILE = os.path.join(DATA_DIR, "sub_tiers.json")
SUBSCRIBERS_FILE = os.path.join(DATA_DIR, "subscribers.json")
SUB_REVENUE_FILE = os.path.join(DATA_DIR, "sub_revenue.json")
SUB_GOAL_FILE = os.path.join(DATA_DIR, "sub_goal.json")
BRANDING_CONFIG_FILE = os.path.join(DATA_DIR, "branding.json")
HOURS_STREAMED_FILE = os.path.join(DATA_DIR, "hours_streamed.json")
ARCHIVE_RETENTION_DAYS = 365

BITS_VALUE_USD_PER_BIT = 0.01

# Some Twitch UI actions look like gifts but are Bits spends (channel-specific).
# Default: treat one-tap gift redemptions as 100 Bits.
ONE_TAP_GIFT_BITS_COST = 100

CHEERMOTE_CACHE_TTL_SECONDS = 6 * 60 * 60
CHEERMOTE_CACHE_LOCK = threading.Lock()
CHEERMOTE_CACHE = {
    "fetched_at": 0.0,
    "prefixes": None,
}

# Background threads update bits + artist earnings; protect file updates.
# RLock so record_cheer/record_bits_spend can hold the lock across the full
# load-modify-save cycle while the inner helpers also acquire it.
BITS_TRACK_LOCK = threading.RLock()
ARTIST_EARNINGS_LOCK = threading.Lock()
SUB_REVENUE_LOCK = threading.RLock()
SUB_GOAL_LOCK = threading.RLock()
HOURS_STREAMED_LOCK = threading.Lock()

# Session tracking for hours streamed
SESSION_START_TIME = None

# Cross-event bits deduplication: prevents counting bits twice when Twitch sends
# multiple events (e.g., PRIVMSG with bits + USERNOTICE onetapgiftredeemed).
# Key: (username_lower, bits_amount), Value: timestamp
BITS_EVENT_DEDUP_LOCK = threading.Lock()
BITS_EVENT_DEDUP = {}
BITS_EVENT_DEDUP_WINDOW_SECONDS = 3

def should_skip_bits_due_to_recent_global(username, bits_count, update_cache=True):
    """Check if we should skip recording bits due to recent duplicate event.
    
    This is used to prevent double-counting when Twitch sends multiple IRC messages
    for the same cheer event (e.g., both a PRIVMSG with bits and a USERNOTICE).
    
    Args:
        username: The username to check
        bits_count: The amount of bits
        update_cache: If True, updates the cache timestamp (call when actually recording bits)
                     If False, only checks without updating (call when checking for duplicates)
    """
    if not username or bits_count <= 0:
        return False
    
    username_lower = str(username).lower()
    key = (username_lower, int(bits_count))
    now = time.time()
    
    with BITS_EVENT_DEDUP_LOCK:
        last_time = BITS_EVENT_DEDUP.get(key)
        if last_time and (now - last_time) < BITS_EVENT_DEDUP_WINDOW_SECONDS:
            return True
        
        # Only update cache if we're actually going to record this
        if update_cache:
            BITS_EVENT_DEDUP[key] = now
            
            # Cleanup old entries periodically
            if len(BITS_EVENT_DEDUP) > 1000:
                cutoff = now - BITS_EVENT_DEDUP_WINDOW_SECONDS
                old_keys = [k for k, v in BITS_EVENT_DEDUP.items() if v < cutoff]
                for k in old_keys:
                    del BITS_EVENT_DEDUP[k]
    
    return False


def record_bits_event_in_cache(username, bits_count):
    """Record that we processed bits for a user to prevent duplicates.
    
    Call this AFTER successfully recording bits to mark them in the cache.
    """
    if not username or bits_count <= 0:
        return
    
    username_lower = str(username).lower()
    key = (username_lower, int(bits_count))
    now = time.time()
    
    with BITS_EVENT_DEDUP_LOCK:
        BITS_EVENT_DEDUP[key] = now

TIER_VALUES = {
    "1000": 4.19,   # Tier 1: $5.99 USD viewer → Partner 70/30 with Twitch → You get $4.19, Artist gets $1.26 (30% of $4.19)
    "2000": 6.99,   # Tier 2: $9.99 USD viewer → Partner 70/30 with Twitch → You get $6.99, Artist gets $2.10 (30% of $6.99)
    "3000": 17.49,  # Tier 3: $24.99 USD viewer → Partner 70/30 with Twitch → You get $17.49, Artist gets $5.25 (30% of $17.49)
}

# Stable archive schema (avoid CSV header drift when stats keys change).
ARCHIVE_FIELDS = [
    "timestamp",
    "subscribers",
    "followers",
    "bits_today",
    "bits_total",
    "subs_today_revenue",
    "subs_total_revenue",
    "artist_bits_monthly",
    "artist_subs_monthly",
    "artist_gifted_monthly",
    "artist_raise_30",
    "artist_yearly",
    "total_revenue",
    "tiers",
    "gifted_tiers",
]

# Notes:
# - Followers endpoint uses moderator:read:followers
# - Bits are tracked via IRC (requires chat:read)
# - EventSub bits tracking requires bits:read scope
# - EventSub channel point redemption tracking requires channel:read:redemptions
SCOPES = ["channel:read:subscriptions", "moderator:read:followers", "chat:read", "bits:read", "channel:read:redemptions"]

def get_oauth_url(client_id, redirect_uri):
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "token",
        "scope": " ".join(SCOPES)
    }
    return "https://id.twitch.tv/oauth2/authorize?" + urlencode(params)

def save_config(config):
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=2)

def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    return None

def _safe_load_json(path, default):
    """Load JSON from disk, recovering from corrupted/partial files."""
    if not os.path.exists(path):
        return default

    # Try primary file.
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except (OSError, JSONDecodeError):
        pass

    # Try leftover temp file (in case a previous write crashed mid-replace).
    tmp_path = path + ".tmp"
    if os.path.exists(tmp_path):
        try:
            with open(tmp_path, 'r') as f:
                data = json.load(f)
            # Best-effort promote temp to primary.
            try:
                os.replace(tmp_path, path)
            except Exception:
                pass
            return data
        except (OSError, JSONDecodeError):
            pass

    # Quarantine corrupted file so we can start fresh.
    try:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        corrupt_path = path + f".corrupt.{ts}"
        os.replace(path, corrupt_path)
        print(f"WARNING: Corrupted JSON detected; moved to {corrupt_path}")
    except Exception:
        print(f"WARNING: Corrupted JSON detected in {path}; starting fresh")

    return default

def get_user_id(client_id, oauth, username):
    url = "https://api.twitch.tv/helix/users"
    headers = {
        "Client-ID": client_id,
        "Authorization": f"Bearer {oauth}"
    }
    params = {"login": username}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json().get("data", [])
        if data:
            return data[0].get("id")
    return None

def get_subscribers(client_id, oauth, broadcaster_id):
    url = "https://api.twitch.tv/helix/subscriptions"
    headers = {
        "Client-ID": client_id,
        "Authorization": f"Bearer {oauth}"
    }
    # Default page size is 20; use the API's total field instead of len(data).
    params = {"broadcaster_id": broadcaster_id, "first": 1}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return int(response.json().get("total", 0) or 0)
    elif response.status_code == 401:
        print("ERROR: Twitch API token expired. Run --setup to re-authenticate.")
    elif response.status_code == 429:
        print("WARNING: Twitch API rate limit exceeded.")
    return 0

def get_subscribers_list(client_id, oauth, broadcaster_id):
    url = "https://api.twitch.tv/helix/subscriptions"
    headers = {
        "Client-ID": client_id,
        "Authorization": f"Bearer {oauth}"
    }
    params = {"broadcaster_id": broadcaster_id, "first": 100}
    subs = []
    pagination = None

    while True:
        if pagination:
            params["after"] = pagination

        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            break

        data = response.json().get("data", [])
        for sub in data:
            subs.append({
                "user_id": sub.get("user_id"),
                "user_login": sub.get("user_login"),
                "user_name": sub.get("user_name"),
                "tier": sub.get("tier"),
                "is_gift": sub.get("is_gift", False),
                "created_at": sub.get("created_at")
            })

        pagination = response.json().get("pagination", {}).get("cursor")
        if not pagination:
            break

    return subs

def get_sub_tiers(client_id, oauth, broadcaster_id):
    subs = get_subscribers_list(client_id, oauth, broadcaster_id)
    tiers = {"1000": 0, "2000": 0, "3000": 0}
    for sub in subs:
        tier = sub.get("tier", "")
        if tier in tiers:
            tiers[tier] += 1
    return tiers, subs

def load_subscribers():
    return _safe_load_json(SUBSCRIBERS_FILE, {"subs": {}, "history": []})

def save_subscribers(data):
    os.makedirs(DATA_DIR, exist_ok=True)
    # Avoid unbounded growth if a bug/loop spams history.
    try:
        history = data.get("history")
        if isinstance(history, list) and len(history) > 10000:
            data["history"] = history[-10000:]
    except Exception:
        pass
    tmp_path = SUBSCRIBERS_FILE + ".tmp"
    with open(tmp_path, 'w') as f:
        json.dump(data, f, indent=2)
    os.replace(tmp_path, SUBSCRIBERS_FILE)

def sync_subscribers(current_subs):
    data = load_subscribers()

    subs_by_id = data.get("subs", {})
    history = data.get("history", [])

    # First run (or after a reset): baseline without counting revenue.
    # Otherwise we'd incorrectly treat all current active subs as "new".
    if (not isinstance(subs_by_id, dict) or len(subs_by_id) == 0) and (not isinstance(history, list) or len(history) == 0):
        timestamp = datetime.now().isoformat()
        seeded = {}
        for sub in current_subs:
            user_id = str(sub.get("user_id") or "")
            if not user_id:
                continue
            merged = dict(sub)
            merged["status"] = "active"
            merged["started_at"] = timestamp
            merged.setdefault("gifts_received", 0)
            seeded[user_id] = merged

        data["subs"] = seeded
        save_subscribers(data)
        return {
            "total": len(current_subs),
            "new": 0,
            "ended": 0,
            "baseline": True,
            "earned_nongift_revenue": 0.0,
            "earned_gifted_revenue": 0.0,
        }

    # Use user_id as the stable key (Helix returns user_id; not an object id).
    current_ids = {str(sub.get("user_id")) for sub in current_subs if sub.get("user_id")}
    active_stored_ids = {
        str(user_id)
        for user_id, sub in subs_by_id.items()
        if isinstance(sub, dict) and sub.get("status") == "active"
    }

    new_ids = current_ids - active_stored_ids
    ended_ids = active_stored_ids - current_ids

    timestamp = datetime.now().isoformat()

    current_subs_by_id = {
        str(sub.get("user_id")): sub
        for sub in current_subs
        if sub.get("user_id")
    }

    for user_id in new_ids:
        sub = current_subs_by_id.get(user_id, {})
        prev = subs_by_id.get(user_id, {}) if isinstance(subs_by_id.get(user_id, {}), dict) else {}

        event_type = "new"
        if prev.get("status") == "ended":
            event_type = "resub"

        merged = dict(prev)
        merged.update(sub)
        merged["status"] = "active"
        merged["started_at"] = timestamp
        merged.pop("ended_at", None)
        merged.setdefault("gifts_received", 0)
        subs_by_id[user_id] = merged

        data.setdefault("history", []).append({
            "type": event_type,
            "user_id": user_id,
            "user_name": merged.get("user_name"),
            "tier": merged.get("tier"),
            "is_gift": bool(merged.get("is_gift", False)),
            "timestamp": timestamp
        })

    for user_id in ended_ids:
        if user_id in subs_by_id and isinstance(subs_by_id[user_id], dict):
            if subs_by_id[user_id].get("status") != "ended":
                subs_by_id[user_id]["status"] = "ended"
                subs_by_id[user_id]["ended_at"] = timestamp
                data.setdefault("history", []).append({
                    "type": "ended",
                    "user_id": user_id,
                    "user_name": subs_by_id[user_id].get("user_name"),
                    "timestamp": timestamp
                })

    data["subs"] = subs_by_id

    save_subscribers(data)

    # Revenue earned comes from newly detected subs only; expirations do not subtract.
    earned_nongift_revenue = 0.0
    earned_gifted_revenue = 0.0
    for user_id in new_ids:
        sub = current_subs_by_id.get(user_id, {})
        tier = str(sub.get("tier") or "")
        price = float(TIER_VALUES.get(tier, 0.0) or 0.0)
        if price <= 0:
            continue
        if bool(sub.get("is_gift", False)):
            earned_gifted_revenue += price
        else:
            earned_nongift_revenue += price

    if len(new_ids) > 0:
        print(f"  + {len(new_ids)} new subscriber(s) detected")
        # Avoid huge startup spam if the file was reset/corrupted.
        max_lines = 25
        shown = 0
        for user_id in sorted(new_ids):
            if shown >= max_lines:
                break
            sub = current_subs_by_id.get(user_id, {})
            tier = sub.get("tier", "unknown")
            print(f"    - {sub.get('user_name', 'Unknown')}: Tier {tier}, Gift: {sub.get('is_gift', False)}")
            shown += 1
        remaining = len(new_ids) - shown
        if remaining > 0:
            print(f"    ... and {remaining} more")

    if len(ended_ids) > 0:
        print(f"  - {len(ended_ids)} subscriber(s) ended")

    return {
        "total": len(current_subs),
        "new": len(new_ids),
        "ended": len(ended_ids),
        "baseline": False,
        "earned_nongift_revenue": round(earned_nongift_revenue, 2),
        "earned_gifted_revenue": round(earned_gifted_revenue, 2),
    }

def load_sub_tiers():
    return _safe_load_json(
        SUB_TIERS_FILE,
        {
            "total_subs_revenue": 0,
            "gifted_subs_revenue": 0,
            "nongift_subs_revenue": 0,
            "tiers": {"1000": 0, "2000": 0, "3000": 0},
            "gifted_tiers": {"1000": 0, "2000": 0, "3000": 0},
        },
    )

def save_sub_tiers(data):
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp_path = SUB_TIERS_FILE + ".tmp"
    with open(tmp_path, 'w') as f:
        json.dump(data, f, indent=2)
    os.replace(tmp_path, SUB_TIERS_FILE)

def load_sub_revenue():
    existed = os.path.exists(SUB_REVENUE_FILE)
    data = _safe_load_json(
        SUB_REVENUE_FILE,
        {
            "date": None,
            "today_total": 0.0,
            "today_nongift": 0.0,
            "today_gifted": 0.0,
            "lifetime_total": 0.0,
            "lifetime_nongift": 0.0,
            "lifetime_gifted": 0.0,
        },
    )

    # One-time best-effort seed for existing installs (carry forward the old
    # displayed subs total as a baseline so totals don't drop to $0.00).
    if not existed:
        try:
            legacy = _safe_load_json(SUB_TIERS_FILE, {})
            baseline = float(legacy.get("total_subs_revenue", 0.0) or 0.0)
            if baseline > 0:
                data["lifetime_total"] = round(baseline, 2)
                data["lifetime_nongift"] = round(baseline, 2)
                data["lifetime_gifted"] = 0.0
        except Exception:
            pass

        # Ensure the file exists immediately (useful for overlays/debugging).
        try:
            save_sub_revenue(data)
        except Exception:
            pass

    today = get_current_date_key()
    if data.get("date") != today:
        data["date"] = today
        data["today_total"] = 0.0
        data["today_nongift"] = 0.0
        data["today_gifted"] = 0.0

    # Backwards compatibility / key safety.
    data.setdefault("today_total", 0.0)
    data.setdefault("today_nongift", 0.0)
    data.setdefault("today_gifted", 0.0)
    data.setdefault("lifetime_total", 0.0)
    data.setdefault("lifetime_nongift", 0.0)
    data.setdefault("lifetime_gifted", 0.0)
    return data

def save_sub_revenue(data):
    with SUB_REVENUE_LOCK:
        os.makedirs(DATA_DIR, exist_ok=True)
        tmp_path = SUB_REVENUE_FILE + ".tmp"
        with open(tmp_path, 'w') as f:
            json.dump(data, f, indent=2)
        os.replace(tmp_path, SUB_REVENUE_FILE)

def _get_month_key(dt=None):
    if dt is None:
        dt = datetime.now()
    return dt.strftime("%Y-%m")

def load_sub_goal():
    existed = os.path.exists(SUB_GOAL_FILE)
    data = _safe_load_json(
        SUB_GOAL_FILE,
        {
            "month": _get_month_key(),
            "current": 0,
            "goal": 150,
            "goal_a": 150,
            "goal_b": 100,
        },
    )

    month = _get_month_key()
    if data.get("month") != month:
        data["month"] = month
        data["current"] = 0
        data["goal"] = int(data.get("goal_a", 150) or 150)

    # Backwards compatibility / key safety.
    data.setdefault("goal_a", 150)
    data.setdefault("goal_b", 100)
    data.setdefault("goal", int(data.get("goal_a", 150) or 150))
    data.setdefault("current", 0)

    # Ensure the file exists so overlays can fetch it.
    if not existed:
        try:
            save_sub_goal(data)
        except Exception:
            pass
    return data

def save_sub_goal(data):
    with SUB_GOAL_LOCK:
        os.makedirs(DATA_DIR, exist_ok=True)
        tmp_path = SUB_GOAL_FILE + ".tmp"
        with open(tmp_path, 'w') as f:
            json.dump(data, f, indent=2)
        os.replace(tmp_path, SUB_GOAL_FILE)

def load_hours_streamed():
    """Load hours streamed data from file."""
    with HOURS_STREAMED_LOCK:
        return _safe_load_json(HOURS_STREAMED_FILE, {
            "total_hours": 0.0,
            "monthly_hours": {},
            "daily_hours": {},
            "session_start": None,
            "last_update": None
        })

def save_hours_streamed(data):
    """Save hours streamed data to file."""
    with HOURS_STREAMED_LOCK:
        os.makedirs(DATA_DIR, exist_ok=True)
        tmp_path = HOURS_STREAMED_FILE + ".tmp"
        with open(tmp_path, 'w') as f:
            json.dump(data, f, indent=2)
        os.replace(tmp_path, HOURS_STREAMED_FILE)

def start_stream_session():
    """Mark the start of a streaming session."""
    global SESSION_START_TIME
    SESSION_START_TIME = time.time()

    data = load_hours_streamed()
    data["session_start"] = datetime.now().isoformat()
    save_hours_streamed(data)
    print(f"  Stream session started at {data['session_start']}")

def update_hours_streamed():
    """Update hours streamed based on current session duration."""
    global SESSION_START_TIME

    if SESSION_START_TIME is None:
        return load_hours_streamed()

    now = time.time()
    session_duration_hours = (now - SESSION_START_TIME) / 3600.0

    data = load_hours_streamed()

    # Get current date and month keys
    current_date = datetime.now().strftime("%Y-%m-%d")
    current_month = datetime.now().strftime("%Y-%m")

    # Initialize if needed
    if "daily_hours" not in data:
        data["daily_hours"] = {}
    if "monthly_hours" not in data:
        data["monthly_hours"] = {}

    # Calculate hours since last update
    last_update = data.get("last_update")
    if last_update:
        try:
            last_update_time = datetime.fromisoformat(last_update)
            # Only count hours from this session
            hours_since_last = (datetime.now() - last_update_time).total_seconds() / 3600.0
            hours_to_add = min(hours_since_last, 0.1)  # Cap at poll interval worth of hours
        except Exception:
            hours_to_add = 0
    else:
        hours_to_add = 0

    # Update totals
    data["total_hours"] = round(float(data.get("total_hours", 0)) + hours_to_add, 2)
    data["daily_hours"][current_date] = round(float(data["daily_hours"].get(current_date, 0)) + hours_to_add, 2)
    data["monthly_hours"][current_month] = round(float(data["monthly_hours"].get(current_month, 0)) + hours_to_add, 2)
    data["last_update"] = datetime.now().isoformat()
    data["current_session_hours"] = round(session_duration_hours, 2)

    save_hours_streamed(data)
    return data

def get_hours_streamed():
    """Get current hours streamed stats."""
    data = load_hours_streamed()
    current_month = datetime.now().strftime("%Y-%m")
    current_date = datetime.now().strftime("%Y-%m-%d")

    return {
        "total_hours": round(float(data.get("total_hours", 0)), 2),
        "monthly_hours": round(float(data.get("monthly_hours", {}).get(current_month, 0)), 2),
        "today_hours": round(float(data.get("daily_hours", {}).get(current_date, 0)), 2),
        "current_session_hours": round(float(data.get("current_session_hours", 0)), 2)
    }

def increment_sub_goal(count=1):
    """Increment the subscriber goal counter (never decreases).

    Rolls over between goal_a (150) and goal_b (100) while carrying remainder.
    """
    try:
        inc = int(count)
    except Exception:
        inc = 0
    if inc <= 0:
        return load_sub_goal()

    with SUB_GOAL_LOCK:
        data = load_sub_goal()
        current = int(data.get("current", 0) or 0)
        goal_a = int(data.get("goal_a", 150) or 150)
        goal_b = int(data.get("goal_b", 100) or 100)
        goal = int(data.get("goal", goal_a) or goal_a)
        if goal <= 0:
            goal = goal_a if goal_a > 0 else 150

        current += inc

        # Carry remainder if we cross the goal.
        while current >= goal and goal > 0:
            current -= goal
            goal = goal_b if goal == goal_a else goal_a

        data["current"] = current
        data["goal"] = goal
        save_sub_goal(data)
        return data

def update_sub_revenue(earned_nongift_revenue, earned_gifted_revenue):
    """Accumulate earned subscription revenue (never decreases on expiry)."""
    try:
        earned_nongift_revenue = float(earned_nongift_revenue or 0.0)
    except Exception:
        earned_nongift_revenue = 0.0
    try:
        earned_gifted_revenue = float(earned_gifted_revenue or 0.0)
    except Exception:
        earned_gifted_revenue = 0.0

    if earned_nongift_revenue < 0:
        earned_nongift_revenue = 0.0
    if earned_gifted_revenue < 0:
        earned_gifted_revenue = 0.0

    with SUB_REVENUE_LOCK:
        data = load_sub_revenue()
        inc_total = earned_nongift_revenue + earned_gifted_revenue

        data["today_nongift"] = round(float(data.get("today_nongift", 0.0) or 0.0) + earned_nongift_revenue, 2)
        data["today_gifted"] = round(float(data.get("today_gifted", 0.0) or 0.0) + earned_gifted_revenue, 2)
        data["today_total"] = round(float(data.get("today_total", 0.0) or 0.0) + inc_total, 2)

        data["lifetime_nongift"] = round(float(data.get("lifetime_nongift", 0.0) or 0.0) + earned_nongift_revenue, 2)
        data["lifetime_gifted"] = round(float(data.get("lifetime_gifted", 0.0) or 0.0) + earned_gifted_revenue, 2)
        data["lifetime_total"] = round(float(data.get("lifetime_total", 0.0) or 0.0) + inc_total, 2)

        save_sub_revenue(data)
        return data

def count_gifted_subs(subs):
    gifted = {"1000": 0, "2000": 0, "3000": 0}
    for sub in subs:
        if sub.get("is_gift", False):
            tier = sub.get("tier", "")
            if tier in gifted:
                gifted[tier] += 1
    return gifted

def get_followers(client_id, oauth, broadcaster_id):
    url = "https://api.twitch.tv/helix/channels/followers"
    headers = {
        "Client-ID": client_id,
        "Authorization": f"Bearer {oauth}"
    }
    params = {"broadcaster_id": broadcaster_id}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json().get("total", 0)
    elif response.status_code == 401:
        print("ERROR: Missing scope for followers (moderator:read:followers) or token invalid. Re-run --setup.")
    elif response.status_code == 429:
        print("WARNING: Twitch API rate limit exceeded (followers).")
    return 0

def get_cheermote_prefixes(client_id, oauth, broadcaster_id):
    """Fetch cheermote prefixes so we can parse cheermote tokens in chat.

    Used as a fallback when IRC doesn't include a bits= tag.
    """
    now = time.time()
    with CHEERMOTE_CACHE_LOCK:
        cached = CHEERMOTE_CACHE.get("prefixes")
        fetched_at = float(CHEERMOTE_CACHE.get("fetched_at", 0.0) or 0.0)
        if cached is not None and (now - fetched_at) < CHEERMOTE_CACHE_TTL_SECONDS:
            return cached

    if not client_id or not oauth or not broadcaster_id:
        return ["cheer", "dino"]

    url = "https://api.twitch.tv/helix/bits/cheermotes"
    headers = {
        "Client-ID": client_id,
        "Authorization": f"Bearer {oauth}",
    }
    params = {"broadcaster_id": broadcaster_id}

    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
    except Exception:
        return ["cheer", "dino"]

    if response.status_code != 200:
        return ["cheer", "dino"]

    try:
        payload = response.json() or {}
    except Exception:
        return ["cheer", "dino"]

    prefixes = set()
    for item in payload.get("data", []) or []:
        p = str(item.get("prefix") or "").strip()
        if p:
            prefixes.add(p.lower())

    # Always support the default cheer token.
    prefixes.add("cheer")
    # Channel-specific: viewer UI commonly shows dino cheermotes.
    prefixes.add("dino")

    out = sorted(prefixes)
    with CHEERMOTE_CACHE_LOCK:
        CHEERMOTE_CACHE["prefixes"] = out
        CHEERMOTE_CACHE["fetched_at"] = now
    return out

def load_bits_tracker():
    with BITS_TRACK_LOCK:
        if os.path.exists(BITS_TRACK_FILE):
            try:
                with open(BITS_TRACK_FILE, 'r') as f:
                    data = json.load(f)
            except Exception:
                data = {}
        else:
            data = {}

        # Backwards compatibility: older versions only stored lifetime_bits_revenue.
        if "lifetime_bits_revenue_tracked" not in data and "lifetime_bits_revenue" in data:
            data["lifetime_bits_revenue_tracked"] = float(data.get("lifetime_bits_revenue", 0.0) or 0.0)
            data["lifetime_bits_revenue_imported"] = float(data.get("lifetime_bits_revenue_imported", 0.0) or 0.0)
            data["lifetime_bits_revenue"] = round(
                float(data.get("lifetime_bits_revenue_tracked", 0.0) or 0.0)
                + float(data.get("lifetime_bits_revenue_imported", 0.0) or 0.0),
                2,
            )

        today = get_current_date_key()
        if data.get("date") != today:
            data["date"] = today
            data["today_bits_count"] = 0
            data["today_bits_revenue"] = 0.0

        data.setdefault("lifetime_bits_count", 0)
        data.setdefault("lifetime_bits_revenue_tracked", 0.0)
        data.setdefault("lifetime_bits_revenue_imported", 0.0)
        data["lifetime_bits_revenue"] = round(
            float(data.get("lifetime_bits_revenue_tracked", 0.0) or 0.0)
            + float(data.get("lifetime_bits_revenue_imported", 0.0) or 0.0),
            2,
        )
        return data

def save_bits_tracker(data):
    with BITS_TRACK_LOCK:
        os.makedirs(DATA_DIR, exist_ok=True)
        tmp_path = BITS_TRACK_FILE + ".tmp"
        with open(tmp_path, 'w') as f:
            json.dump(data, f, indent=2)
        os.replace(tmp_path, BITS_TRACK_FILE)

def record_cheer(bits_count):
    try:
        bits_count = int(bits_count)
    except Exception:
        return
    if bits_count <= 0:
        return

    revenue = bits_count * BITS_VALUE_USD_PER_BIT

    # Hold the lock for the full load-modify-save to prevent concurrent
    # calls (IRC + EventSub threads) from overwriting each other's updates.
    with BITS_TRACK_LOCK:
        data = load_bits_tracker()
        data["today_bits_count"] = int(data.get("today_bits_count", 0) or 0) + bits_count
        data["today_bits_revenue"] = round(float(data.get("today_bits_revenue", 0.0) or 0.0) + revenue, 2)
        data["lifetime_bits_count"] = int(data.get("lifetime_bits_count", 0) or 0) + bits_count
        data["lifetime_bits_revenue_tracked"] = round(float(data.get("lifetime_bits_revenue_tracked", 0.0) or 0.0) + revenue, 2)
        data["lifetime_bits_revenue"] = round(
            float(data.get("lifetime_bits_revenue_tracked", 0.0) or 0.0)
            + float(data.get("lifetime_bits_revenue_imported", 0.0) or 0.0),
            2,
        )
        save_bits_tracker(data)

    # Update the artist fund in real-time for bits revenue.
    try:
        update_artist_earnings(revenue * 0.30, 0.0, 0.0)
    except Exception:
        pass

    if bits_count == 1:
        bits_label = "1 bit"
    else:
        bits_label = f"{bits_count} bits"
    print(f"  + Cheer detected: {bits_label} (${revenue:.2f})")

def record_bits_spend(bits_count, label, username=None, skip_duplicate_check=False):
    """Record Bits that were spent (not necessarily a Cheer).

    Updates the Bits tracker + artist earnings just like a cheer.
    
    Args:
        bits_count: Number of bits spent
        label: Description of the spend (e.g., "One-Tap Gift")
        username: Optional username for deduplication (prevents double-counting)
        skip_duplicate_check: If True, skip the duplicate check (use when already checked)
    """
    try:
        bits_count = int(bits_count)
    except Exception:
        return
    if bits_count <= 0:
        return

    # Check for duplicate events if username provided and not already checked
    if username and not skip_duplicate_check:
        if should_skip_bits_due_to_recent_global(username, bits_count, update_cache=False):
            print(f"  + {label} skipped (duplicate for {username}: {bits_count} bits)")
            return
    
    # Record this bits event in cache to prevent duplicates
    if username:
        record_bits_event_in_cache(username, bits_count)

    revenue = bits_count * BITS_VALUE_USD_PER_BIT

    # Hold the lock for the full load-modify-save to prevent concurrent
    # calls from overwriting each other's updates.
    with BITS_TRACK_LOCK:
        data = load_bits_tracker()
        data["today_bits_count"] = int(data.get("today_bits_count", 0) or 0) + bits_count
        data["today_bits_revenue"] = round(float(data.get("today_bits_revenue", 0.0) or 0.0) + revenue, 2)
        data["lifetime_bits_count"] = int(data.get("lifetime_bits_count", 0) or 0) + bits_count
        data["lifetime_bits_revenue_tracked"] = round(float(data.get("lifetime_bits_revenue_tracked", 0.0) or 0.0) + revenue, 2)
        data["lifetime_bits_revenue"] = round(
            float(data.get("lifetime_bits_revenue_tracked", 0.0) or 0.0)
            + float(data.get("lifetime_bits_revenue_imported", 0.0) or 0.0),
            2,
        )
        save_bits_tracker(data)

    try:
        update_artist_earnings(revenue * 0.30, 0.0, 0.0)
    except Exception:
        pass

    print(f"  + {label}: {bits_count} bits (${revenue:.2f})")

def _normalize_sub_plan_to_tier(plan):
    """Map IRC/EventSub plan values to our tier keys (1000/2000/3000)."""
    if plan is None:
        return "1000"
    plan = str(plan).strip()
    if not plan:
        return "1000"
    low = plan.lower()
    if low == "prime":
        return "1000"
    if plan in ("1000", "2000", "3000"):
        return plan
    return "1000"

def record_sub_event_from_irc(tags):
    """Count sub earnings events from Twitch IRC USERNOTICE tags.

    Tracks:
    - msg-id=sub (new paid sub)
    - msg-id=resub (renewal)
    - msg-id=subgift / anonsubgift (single gifted sub)
    - msg-id=submysterygift / anonsubmysterygift (gift bombs)

    Notes:
    - Prime counts as Tier 1 for revenue purposes.
    - We only ever increment earnings; nothing here subtracts.
    """
    try:
        msg_id = str(tags.get("msg-id") or "")
        if msg_id not in {
            "sub",
            "resub",
            "subgift",
            "anonsubgift",
            "submysterygift",
            "anonsubmysterygift",
            "onetapgiftredeemed",
        }:
            return

        # Ignore gift-bomb summary events for earnings to avoid double counting.
        # We count the individual subgift/anonsubgift events instead.
        if msg_id in {"submysterygift", "anonsubmysterygift"}:
            tier = _normalize_sub_plan_to_tier(tags.get("msg-param-sub-plan"))
            print(f"  + Gift Bomb detected: Tier {tier} (ignored for earnings)")
            return

        # If Twitch also emits a sub event for a gifted subscription, ignore it.
        # We count the gift event instead.
        if msg_id == "sub":
            was_gifted = str(tags.get("msg-param-was-gifted") or "").lower()
            if was_gifted in {"1", "true", "yes"}:
                return

        # One-tap gift redemptions: treat as Bits spend (not subscription revenue).
        # Only increment goal and record bits if this isn't a duplicate event.
        if msg_id == "onetapgiftredeemed":
            # Get username for deduplication (prevents double-counting when Twitch
            # sends both a PRIVMSG with bits and this USERNOTICE)
            username = str(tags.get("display-name") or tags.get("login") or tags.get("msg-param-recipient-user-name") or "")
            
            # Check if we already recorded bits for this user recently (without updating cache)
            # If so, skip both the goal increment and bits recording
            if not should_skip_bits_due_to_recent_global(username, ONE_TAP_GIFT_BITS_COST, update_cache=False):
                try:
                    increment_sub_goal(1)
                except Exception:
                    pass
                # Pass skip_duplicate_check=True since we already checked above
                record_bits_spend(int(ONE_TAP_GIFT_BITS_COST), "One-Tap Gift", username, skip_duplicate_check=True)
            else:
                print(f"  + One-Tap Gift skipped (duplicate for {username})")
            return

        tier = _normalize_sub_plan_to_tier(tags.get("msg-param-sub-plan"))
        price = float(TIER_VALUES.get(tier, 0.0) or 0.0)
        if price <= 0:
            return

        is_gift = msg_id in {
            "subgift",
            "anonsubgift",
            "onetapgiftredeemed",
        }

        earned_nongift = 0.0
        earned_gifted = 0.0
        if is_gift:
            earned_gifted = price
        else:
            earned_nongift = price

        try:
            update_sub_revenue(earned_nongift, earned_gifted)
        except Exception:
            pass

        # Subscriber goal progress is based on earned sub events (never decreases).
        try:
            increment_sub_goal(1)
        except Exception:
            pass

        try:
            update_artist_earnings(0.0, earned_nongift * 0.30, earned_gifted * 0.30)
        except Exception:
            pass

        label = {
            "sub": "Sub",
            "resub": "Resub",
            "subgift": "Gift Sub",
            "anonsubgift": "Anon Gift Sub",
            "onetapgiftredeemed": "One-Tap Gift",
        }.get(msg_id, msg_id)
        dollars = (earned_nongift + earned_gifted)
        print(f"  + {label} detected: Tier {tier} (${dollars:.2f})")
    except Exception:
        return

def record_gigantify_from_irc(tags):
    """Record the Gigantify Emote power-up spend (configured as 50 bits)."""
    # Twitch doesn't reliably include the Bits amount in IRC tags for power-ups.
    # The streamer sets the price; for this channel it's fixed at 50 bits.
    record_bits_spend(50, "Gigantify power-up")

def _is_gigantify_powerup(tags):
    """Best-effort detection of Gigantify events in IRC tags."""
    try:
        # Some clients/events surface this as msg-id-like values.
        msg_id = str(tags.get("msg-id") or "")
        low_msg_id = msg_id.lower()
        if msg_id in {
            "power_ups_gigantified_emote",
            "power-ups-gigantified-emote",
            "gigantified_emote",
            "gigantified-emote",
            "gigantify_emote",
            "gigantify-emote",
        }:
            return True

        if "gigant" in low_msg_id:
            return True

        # Power-ups are often exposed via the animation-id tag on chat messages.
        anim = str(tags.get("animation-id") or "")
        low_anim = anim.lower()
        if anim in {
            "power_ups_gigantified_emote",
            "gigantified_emote",
            "gigantified-emote",
        }:
            return True

        if "gigant" in low_anim:
            return True
    except Exception:
        return False
    return False

def _parse_irc_tags(tag_str):
    tags = {}
    for part in tag_str.split(';'):
        if '=' in part:
            k, v = part.split('=', 1)
            tags[k] = v
        else:
            tags[part] = ""
    return tags

def _extract_privmsg_text(rest):
    # Format: ":user!user@user.tmi.twitch.tv PRIVMSG #channel :message"
    try:
        if " PRIVMSG " not in rest:
            return ""
        if " :" not in rest:
            return ""
        return rest.split(" :", 1)[1]
    except Exception:
        return ""

def _parse_cheermote_bits_from_text(text, prefixes):
    if not text or not prefixes:
        return 0

    prefix_set = set(str(p).lower() for p in prefixes if p)
    total = 0

    words = [re.sub(r"^[^A-Za-z0-9]+|[^A-Za-z0-9]+$", "", w) for w in str(text).split()]
    words = [w for w in words if w]

    i = 0
    while i < len(words):
        token = words[i]

        # Pattern 1: dino100 / cheer100
        m = re.match(r"^([A-Za-z]+)(\d+)$", token)
        if m:
            prefix = m.group(1).lower()
            if prefix in prefix_set:
                try:
                    total += int(m.group(2))
                except Exception:
                    pass
            i += 1
            continue

        # Pattern 2: dino 100 (UI sometimes displays prefix + amount separately)
        if token.isalpha() and token.lower() in prefix_set and i + 1 < len(words):
            nxt = words[i + 1]
            if nxt.isdigit():
                try:
                    total += int(nxt)
                except Exception:
                    pass
                i += 2
                continue

        i += 1
    return total

def start_bits_irc_listener(config):
    """Background listener that tracks cheers (bits) via Twitch IRC."""
    token = (config or {}).get("access_token")
    username = (config or {}).get("username")
    channel = (config or {}).get("channel") or username
    client_id = (config or {}).get("client_id")
    broadcaster_id = (config or {}).get("broadcaster_id")
    if not token or not username or not channel:
        missing = []
        if not token:
            missing.append("access_token")
        if not username:
            missing.append("username")
        if not channel:
            missing.append("channel")
        print("WARNING: IRC bits tracker disabled (missing: " + ", ".join(missing) + ").")
        return

    debug_irc = bool((config or {}).get("debug_irc", False))

    def run():
        server = "irc.chat.twitch.tv"
        port = 6697
        backoff = 2

        # Deduplicate IRC messages by their unique ID (best-effort).
        seen_msg_ids = set()
        seen_order = []
        max_seen = 5000

        # Note: Cross-event deduplication is now handled globally via
        # should_skip_bits_due_to_recent_global() to prevent double-counting
        # when Twitch sends multiple events for the same cheer.

        while True:
            s = None
            try:
                raw = socket.create_connection((server, port), timeout=15)
                ctx = ssl.create_default_context()
                s = ctx.wrap_socket(raw, server_hostname=server)
                # Twitch IRC may be idle for long periods (no chat activity).
                # Use a generous timeout and don't treat read timeouts as fatal.
                s.settimeout(600)

                sock = s

                def send(line):
                    sock.sendall((line + "\r\n").encode("utf-8"))

                send("CAP REQ :twitch.tv/tags twitch.tv/commands twitch.tv/membership")
                send(f"PASS oauth:{token}")
                send(f"NICK {str(username).lower()}")
                send(f"JOIN #{channel.lower()}")

                print(f"IRC bits tracker connected: #{channel.lower()}")
                backoff = 2

                # Best-effort: load cheermote prefixes for parsing cheermote tokens.
                cheer_prefixes = get_cheermote_prefixes(client_id, token, broadcaster_id) or ["cheer"]
                if debug_irc:
                    try:
                        print("IRC DEBUG: cheermote prefixes loaded=" + str(len(cheer_prefixes)))
                    except Exception:
                        pass

                buf = b""
                while True:
                    try:
                        chunk = s.recv(4096)
                    except socket.timeout:
                        # Idle connection; keep waiting.
                        continue

                    if not chunk:
                        raise ConnectionError("IRC connection closed")

                    buf += chunk
                    while b"\r\n" in buf:
                        line, buf = buf.split(b"\r\n", 1)
                        try:
                            msg = line.decode("utf-8", errors="ignore")
                        except Exception:
                            continue

                        if msg.startswith("PING"):
                            send("PONG :tmi.twitch.tv")
                            continue

                        # Surface server notices (auth failures, etc.).
                        if " NOTICE " in msg or msg.startswith(":tmi.twitch.tv NOTICE"):
                            print(f"IRC NOTICE: {msg}")
                            low = msg.lower()
                            if "login authentication failed" in low or "improperly formatted auth" in low:
                                print("WARNING: IRC auth failed. Re-run: python twitch_stats.py --setup (ensure chat:read scope).")
                                return

                        if msg.startswith("@"): 
                            try:
                                tag_part, rest = msg.split(" ", 1)
                            except ValueError:
                                continue

                            raw_tags = tag_part[1:]
                            tags = _parse_irc_tags(raw_tags)

                            msg_id = str(tags.get("msg-id") or "")

                            # Cheers should include a bits= tag, but to avoid missing
                            # any cheers due to tag parsing edge-cases, also parse it
                            # directly from the raw tags.
                            bits = tags.get("bits")
                            if not bits and "bits=" in raw_tags:
                                try:
                                    for part in raw_tags.split(";"):
                                        if part.startswith("bits="):
                                            bits = part.split("=", 1)[1]
                                            break
                                except Exception:
                                    bits = None

                            if debug_irc:
                                anim = tags.get("animation-id")
                                # Print when we see any relevant signal.
                                if bits or anim or msg_id:
                                    print(
                                        "IRC DEBUG: id="
                                        + str(tags.get("id"))
                                        + " msg-id="
                                        + str(msg_id)
                                        + " animation-id="
                                        + str(anim)
                                        + " bits="
                                        + str(bits)
                                    )

                            # Power-ups (Gigantify) spend bits but are not Cheers.
                            # Detect via msg-id and/or animation-id.
                            if _is_gigantify_powerup(tags):
                                irc_id = tags.get("id")
                                if irc_id:
                                    if irc_id in seen_msg_ids:
                                        continue
                                    seen_msg_ids.add(irc_id)
                                    seen_order.append(irc_id)
                                    if len(seen_order) > max_seen:
                                        old = seen_order.pop(0)
                                        try:
                                            seen_msg_ids.remove(old)
                                        except KeyError:
                                            pass
                                record_gigantify_from_irc(tags)

                            # Count renewals (resubs). These are emitted as USERNOTICE with msg-id=resub.
                            if msg_id in {
                                "sub",
                                "resub",
                                "subgift",
                                "anonsubgift",
                                "submysterygift",
                                "anonsubmysterygift",
                                "onetapgiftredeemed",
                            }:
                                irc_id = tags.get("id")
                                if irc_id:
                                    if irc_id in seen_msg_ids:
                                        continue
                                    seen_msg_ids.add(irc_id)
                                    seen_order.append(irc_id)
                                    if len(seen_order) > max_seen:
                                        old = seen_order.pop(0)
                                        try:
                                            seen_msg_ids.remove(old)
                                        except KeyError:
                                            pass
                                record_sub_event_from_irc(tags)

                            # Bits cheers (and fallback parsing for cheermote tokens).
                            # IMPORTANT: Use message ID deduplication to prevent double-counting
                            # when both the bits tag and text parsing match the same cheer.
                            # ALSO: Cross-event deduplication for username+amount within time window.
                            bits_recorded = False
                            cheer_username = str(tags.get("display-name") or tags.get("login") or "").lower()
                            if bits:
                                irc_id = tags.get("id")
                                if irc_id:
                                    if irc_id in seen_msg_ids:
                                        continue
                                    seen_msg_ids.add(irc_id)
                                    seen_order.append(irc_id)
                                    if len(seen_order) > max_seen:
                                        old = seen_order.pop(0)
                                        try:
                                            seen_msg_ids.remove(old)
                                        except KeyError:
                                            pass
                                try:
                                    bits_count = int(bits)
                                    if not should_skip_bits_due_to_recent_global(cheer_username, bits_count):
                                        record_cheer(bits_count)
                                        bits_recorded = True
                                    else:
                                        if debug_irc:
                                            print(f"IRC DEBUG: skipping duplicate bits event for {cheer_username}: {bits_count} bits (within {BITS_EVENT_DEDUP_WINDOW_SECONDS}s window)")
                                except Exception:
                                    record_cheer(bits)
                                    bits_recorded = True
                            else:
                                parsed = _parse_cheermote_bits_from_text(_extract_privmsg_text(rest), cheer_prefixes)
                                if parsed > 0:
                                    irc_id = tags.get("id")
                                    if irc_id:
                                        if irc_id in seen_msg_ids:
                                            continue
                                        seen_msg_ids.add(irc_id)
                                        seen_order.append(irc_id)
                                        if len(seen_order) > max_seen:
                                            old = seen_order.pop(0)
                                            try:
                                                seen_msg_ids.remove(old)
                                            except KeyError:
                                                pass
                                    if not should_skip_bits_due_to_recent_global(cheer_username, parsed):
                                        if debug_irc:
                                            print("IRC DEBUG: parsed cheermote bits=" + str(parsed))
                                        record_cheer(parsed)
                                        bits_recorded = True
                                    else:
                                        if debug_irc:
                                            print(f"IRC DEBUG: skipping duplicate cheermote bits for {cheer_username}: {parsed} bits (within {BITS_EVENT_DEDUP_WINDOW_SECONDS}s window)")
                                elif debug_irc:
                                    # Helpful when Twitch UI shows a cheer but the IRC message
                                    # didn't include bits= (or we failed to parse it).
                                    low_rest = (rest or "").lower()
                                    if "privmsg" in low_rest and "cheer" in low_rest:
                                        print("IRC DEBUG: cheer-like PRIVMSG without bits tag")
                        else:
                            # Some messages may arrive without tags. As a last resort,
                            # attempt to parse cheermote tokens from the message body.
                            # These messages have no ID, so we can't deduplicate them.
                            if " PRIVMSG " in msg and " :" in msg:
                                parsed = _parse_cheermote_bits_from_text(_extract_privmsg_text(msg), cheer_prefixes)
                                if parsed > 0:
                                    if debug_irc:
                                        print("IRC DEBUG: parsed cheermote bits (no tags)=" + str(parsed))
                                    record_cheer(parsed)
            except Exception as e:
                print(f"WARNING: IRC bits tracker error: {e}. Reconnecting...")
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
            finally:
                try:
                    if s:
                        s.close()
                except Exception:
                    pass

    t = threading.Thread(target=run, daemon=True)
    t.start()

def start_eventsub_websocket_listener(config):
    """Background listener that tracks bits via EventSub WebSocket.

    This catches ALL bits events including small cheermotes (1-99 bits)
    that don't appear in IRC chat.

    Requires: pip install websocket-client
    """
    try:
        import websocket
    except ImportError:
        print("INFO: EventSub WebSocket not available (install: pip install websocket-client)")
        return

    token = (config or {}).get("access_token")
    client_id = (config or {}).get("client_id")
    broadcaster_id = (config or {}).get("broadcaster_id")
    debug_eventsub = bool((config or {}).get("debug_eventsub", False))

    if not token or not client_id or not broadcaster_id:
        print("WARNING: EventSub WebSocket disabled (missing credentials).")
        return

    def run():
        backoff = 2
        session_id = None
        reconnect_url = None
        subscribed = False

        processed_events = set()

        def on_message(ws, message):
            nonlocal session_id, reconnect_url, subscribed
            try:
                data = json.loads(message)
                msg_type = data.get("metadata", {}).get("message_type")
                message_id = data.get("metadata", {}).get("message_id", "")

                if debug_eventsub:
                    print(f"EventSub DEBUG: msg_type={msg_type}, msg_id={message_id[:16] if message_id else 'none'}")

                if msg_type == "session_welcome":
                    session_id = data["payload"]["session"]["id"]
                    if debug_eventsub:
                        print(f"EventSub DEBUG: session_welcome, session_id={session_id[:16]}")
                    print(f"EventSub WebSocket connected, session: {session_id[:8]}...")

                    # Subscribe to channel.bits.use — catches ALL bits usage:
                    # cheers, power-ups, combos, animated cheermotes (heart/dino).
                    subscribe_to_bits_use(token, client_id, broadcaster_id, session_id)
                    # Subscribe to channel.cheer as a fallback for older Twitch behavior
                    subscribe_to_cheer(token, client_id, broadcaster_id, session_id)
                    # Subscribe to channel point redemption events
                    subscribe_to_channel_points(token, client_id, broadcaster_id, session_id)

                elif msg_type == "session_reconnect":
                    reconnect_url = data["payload"]["session"]["connection_url"]
                    subscribed = False  # Need to resubscribe after reconnect
                    if debug_eventsub:
                        print(f"EventSub DEBUG: session_reconnect, url={reconnect_url[:50]}...")
                    ws.close()

                elif msg_type == "session_keepalive":
                    pass

                elif msg_type == "notification":
                    subscription_type = data["metadata"]["subscription_type"]

                    if debug_eventsub:
                        print(f"EventSub DEBUG: notification received - type={subscription_type}")
                        print(f"EventSub DEBUG: full event data={json.dumps(data, indent=2)[:800]}")

                    event_id = message_id

                    if event_id in processed_events:
                        if debug_eventsub:
                            print(f"EventSub DEBUG: skipping duplicate event {event_id[:16]}")
                        return
                    processed_events.add(event_id)

                    if len(processed_events) > 1000:
                        processed_events.clear()

                    if subscription_type == "channel.bits.use":
                        # New all-purpose bits event: covers cheers, power-ups,
                        # combos, and animated cheermotes (heart/dino tray items).
                        event = data["payload"]["event"]
                        user_name = event.get("user_name", "Anonymous")
                        bits = event.get("bits", 0)
                        bits_type = event.get("type", "unknown")  # "cheer", "power_up", "combo", etc.

                        if debug_eventsub:
                            print(f"EventSub DEBUG: bits.use from {user_name}, bits={bits}, type={bits_type}, id={event_id[:16]}")

                        if bits > 0:
                            # Cross-event dedup: skip if IRC or channel.cheer already recorded this.
                            if should_skip_bits_due_to_recent_global(user_name, bits):
                                if debug_eventsub:
                                    print(f"EventSub DEBUG: skipping duplicate bits.use for {user_name}: {bits} bits (already recorded)")
                            else:
                                record_cheer(bits)
                                label = {"cheer": "cheer", "power_up": "power-up", "combo": "combo"}.get(bits_type, bits_type)
                                print(f"  + EventSub: {user_name} used {bits} bits ({label})")

                    elif subscription_type == "channel.cheer":
                        event = data["payload"]["event"]
                        user_name = event.get("user_name", "Anonymous")
                        bits = event.get("bits", 0)

                        if debug_eventsub:
                            print(f"EventSub DEBUG: cheer event from {user_name}, bits={bits}, id={event_id[:16]}")

                        if bits > 0:
                            # Cross-event dedup: skip if bits.use or IRC already recorded this.
                            if should_skip_bits_due_to_recent_global(user_name, bits):
                                if debug_eventsub:
                                    print(f"EventSub DEBUG: skipping duplicate cheer for {user_name}: {bits} bits (already recorded)")
                            else:
                                record_cheer(bits)
                                print(f"  + EventSub: {user_name} cheered {bits} bits")

                    elif subscription_type == "channel.channel_points_custom_reward_redemption.add":
                        event = data["payload"]["event"]
                        user_name = event.get("user_name", "Anonymous")
                        reward = event.get("reward", {})
                        reward_title = reward.get("title", "Unknown Reward")

                        if debug_eventsub:
                            print(f"EventSub DEBUG: point redemption from {user_name}, title={reward_title}")

                        bits_cost = event.get("bits_cost", 0) or 0
                        if bits_cost > 0:
                            record_bits_spend(bits_cost, f"Channel Point Reward: {reward_title}", user_name)
                            print(f"  + EventSub: {user_name} redeemed '{reward_title}' for {bits_cost} bits")

            except Exception as e:
                print(f"EventSub error: {e}")
                if debug_eventsub:
                    import traceback
                    traceback.print_exc()

        def on_error(ws, error):
            print(f"EventSub WebSocket error: {error}")

        def on_close(ws, close_status_code, close_msg):
            subscribed = False
            print(f"EventSub WebSocket closed (code={close_status_code}), reconnecting in {backoff}s...")

        def on_open(ws):
            print("EventSub WebSocket connection opened")

        while True:
            try:
                if reconnect_url:
                    ws_url = reconnect_url
                    reconnect_url = None
                    if debug_eventsub:
                        print(f"EventSub: Using reconnect URL")
                else:
                    ws_url = "wss://eventsub.wss.twitch.tv/ws"

                if debug_eventsub:
                    print(f"EventSub: Connecting to {ws_url}...")

                ws = websocket.WebSocketApp(
                    ws_url,
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close
                )
                ws.run_forever()
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
            except Exception as e:
                print(f"EventSub listener error: {e}")
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)

    def subscribe_to_bits_use(token, client_id, broadcaster_id, session_id):
        """Subscribe to channel.bits.use events via EventSub.

        This is the new all-purpose bits event that fires for ALL bits usage:
        cheers, power-ups, combos, and animated cheermotes (heart/dino tray items).
        """
        url = "https://api.twitch.tv/helix/eventsub/subscriptions"
        headers = {
            "Client-ID": client_id,
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        payload = {
            "type": "channel.bits.use",
            "version": "1",
            "condition": {
                "broadcaster_user_id": broadcaster_id
            },
            "transport": {
                "method": "websocket",
                "session_id": session_id
            }
        }

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=10)
            if response.status_code == 202:
                print("  + EventSub: Subscribed to channel.bits.use events (all bits usage)")
            elif response.status_code == 409:
                print("  + EventSub: Already subscribed to channel.bits.use")
            else:
                print(f"EventSub: bits.use subscription response {response.status_code}")
                if debug_eventsub:
                    print(f"Response: {response.text[:500]}")
        except Exception as e:
            print(f"  - EventSub: Error subscribing to bits.use: {e}")

    def subscribe_to_cheer(token, client_id, broadcaster_id, session_id):
        """Subscribe to channel.cheer events via EventSub (fallback)."""
        url = "https://api.twitch.tv/helix/eventsub/subscriptions"
        headers = {
            "Client-ID": client_id,
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        payload = {
            "type": "channel.cheer",
            "version": "1",
            "condition": {
                "broadcaster_user_id": broadcaster_id
            },
            "transport": {
                "method": "websocket",
                "session_id": session_id
            }
        }

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=10)
            if response.status_code == 202:
                print("  + EventSub: Subscribed to channel.cheer events")
            elif response.status_code == 409:
                print("  + EventSub: Already subscribed to channel.cheer")
            else:
                print(f"EventSub: Subscribe response {response.status_code}")
                if debug_eventsub:
                    print(f"Response: {response.text[:500]}")
        except Exception as e:
            print(f"  - EventSub: Error subscribing to cheer: {e}")

    def subscribe_to_channel_points(token, client_id, broadcaster_id, session_id):
        """Subscribe to channel point redemption events via EventSub."""
        url = "https://api.twitch.tv/helix/eventsub/subscriptions"
        headers = {
            "Client-ID": client_id,
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        payload = {
            "type": "channel.channel_points_custom_reward_redemption.add",
            "version": "1",
            "condition": {
                "broadcaster_user_id": broadcaster_id
            },
            "transport": {
                "method": "websocket",
                "session_id": session_id
            }
        }

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=10)
            if response.status_code == 202:
                print("  + EventSub: Subscribed to channel point redemption events")
            elif response.status_code == 409:
                print("  + EventSub: Already subscribed to channel point redemptions")
            else:
                print(f"EventSub: Points subscription response {response.status_code}")
                if debug_eventsub:
                    print(f"Response: {response.text[:500]}")
        except Exception as e:
            print(f"  - EventSub: Error subscribing to channel points: {e}")

    t = threading.Thread(target=run, daemon=True)
    t.start()

def list_eventsub_subscriptions(config):
    """List all EventSub subscriptions for debugging."""
    token = config.get("access_token")
    client_id = config.get("client_id")

    url = "https://api.twitch.tv/helix/eventsub/subscriptions"
    headers = {
        "Client-ID": client_id,
        "Authorization": f"Bearer {token}"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            subs = data.get("data", [])
            print(f"EventSub Subscriptions ({len(subs)} total):")
            for sub in subs:
                sub_type = sub.get("type", "unknown")
                status = sub.get("status", "unknown")
                sub_id = sub.get("id", "")
                condition = sub.get("condition", {})
                transport = sub.get("transport", {})
                print(f"  - {sub_type}: {status} (id={sub_id})")
                if sub_type == "channel.cheer":
                    print(f"    Condition: {condition}")
                elif sub_type == "channel.channel_points_custom_reward_redemption.add":
                    print(f"    Condition: {condition}")
        else:
            print(f"Error: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Error listing subscriptions: {e}")

def delete_eventsub_subscription(config, subscription_id):
    """Delete an EventSub subscription by ID."""
    token = config.get("access_token")
    client_id = config.get("client_id")

    url = f"https://api.twitch.tv/helix/eventsub/subscriptions?id={subscription_id}"
    headers = {
        "Client-ID": client_id,
        "Authorization": f"Bearer {token}"
    }

    try:
        response = requests.delete(url, headers=headers, timeout=10)
        if response.status_code in (204, 200):
            print(f"Deleted subscription {subscription_id}")
        else:
            print(f"Error deleting: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Error deleting subscription: {e}")

def calculate_earnings(bits, donations):
    total_revenue = bits + donations
    creator_earnings = total_revenue * 0.70
    artist_raise = total_revenue * 0.30
    return creator_earnings, artist_raise

def load_artist_earnings():
    return _safe_load_json(ARTIST_EARNINGS_FILE, {"monthly_bits": {}, "monthly_subs": {}, "monthly_gifted": {}, "lifetime_bits": 0, "lifetime_subs": 0, "lifetime_gifted": 0, "lifetime_total": 0})

def save_artist_earnings(data):
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp_path = ARTIST_EARNINGS_FILE + ".tmp"
    with open(tmp_path, 'w') as f:
        json.dump(data, f, indent=2)
    os.replace(tmp_path, ARTIST_EARNINGS_FILE)

def load_daily_bits():
    return _safe_load_json(DAILY_BITS_FILE, {"date": None, "bits": 0, "total_bits": 0})

def save_daily_bits(data):
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp_path = DAILY_BITS_FILE + ".tmp"
    with open(tmp_path, 'w') as f:
        json.dump(data, f, indent=2)
    os.replace(tmp_path, DAILY_BITS_FILE)

def get_current_date_key():
    return datetime.now().strftime("%Y-%m-%d")

def get_current_month_key():
    return datetime.now().strftime("%Y-%m")

def get_archive_filename():
    return os.path.join(ARCHIVE_DIR, f"stats_archive_{get_current_date_key()}.csv")

def cleanup_old_archives():
    cutoff_date = datetime.now() - timedelta(days=ARCHIVE_RETENTION_DAYS)
    for archive_path in glob.glob(os.path.join(ARCHIVE_DIR, "stats_archive_*.csv")):
        date_str = os.path.basename(archive_path).replace("stats_archive_", "").replace(".csv", "")
        try:
            file_date = datetime.strptime(date_str, "%Y-%m-%d")
            if file_date < cutoff_date:
                os.remove(archive_path)
        except ValueError:
            continue

def rotate_archive_if_needed():
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    migrate_old_archive()
    current_archive = get_archive_filename()
    old_archives = glob.glob(os.path.join(ARCHIVE_DIR, "stats_archive_*.csv"))
    for old_path in old_archives:
        if old_path != current_archive:
            try:
                date_str = os.path.basename(old_path).replace("stats_archive_", "").replace(".csv", "")
                datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                os.remove(old_path)
    cleanup_old_archives()

def migrate_old_archive():
    old_archive = os.path.join(DATA_DIR, "stats_archive.csv")
    if os.path.exists(old_archive) and os.path.getsize(old_archive) > 0:
        import shutil
        today = get_current_date_key()
        backup_path = os.path.join(ARCHIVE_DIR, f"stats_archive_{today}.csv")
        if not os.path.exists(backup_path):
            shutil.move(old_archive, backup_path)

def update_daily_bits(current_bits):
    data = load_daily_bits()
    today = get_current_date_key()

    if data["date"] != today:
        data["date"] = today
        data["bits"] = 0

    daily_increment = max(0, current_bits - data["total_bits"])
    data["bits"] += daily_increment
    data["total_bits"] = current_bits

    save_daily_bits(data)
    return data["bits"], daily_increment

def update_artist_earnings(daily_artist_bits, daily_artist_subs, daily_artist_gifted=0.0):
    with ARTIST_EARNINGS_LOCK:
        data = load_artist_earnings()
        current_month = get_current_month_key()

        last_monthly_bits = data["monthly_bits"].get(current_month, 0)
        last_monthly_subs = data["monthly_subs"].get(current_month, 0)
        last_monthly_gifted = data["monthly_gifted"].get(current_month, 0) if "monthly_gifted" in data else 0

        new_monthly_bits = last_monthly_bits + daily_artist_bits
        new_monthly_subs = last_monthly_subs + daily_artist_subs
        new_monthly_gifted = last_monthly_gifted + daily_artist_gifted

        if "monthly_gifted" not in data:
            data["monthly_gifted"] = {}

        data["monthly_bits"][current_month] = round(new_monthly_bits, 2)
        data["monthly_subs"][current_month] = round(new_monthly_subs, 2)
        data["monthly_gifted"][current_month] = round(new_monthly_gifted, 2)

        lifetime_bits = data["lifetime_bits"] + daily_artist_bits
        lifetime_subs = data["lifetime_subs"] + daily_artist_subs
        lifetime_gifted = data.get("lifetime_gifted", 0) + daily_artist_gifted
        lifetime_total = lifetime_bits + lifetime_subs + lifetime_gifted

        data["lifetime_bits"] = round(lifetime_bits, 2)
        data["lifetime_subs"] = round(lifetime_subs, 2)
        data["lifetime_gifted"] = round(lifetime_gifted, 2)
        data["lifetime_total"] = round(lifetime_total, 2)

        save_artist_earnings(data)
        return new_monthly_bits, new_monthly_subs, new_monthly_gifted, data["lifetime_total"]

def update_totals(current_tiers, gifted_tiers):
    # Persist tiers for fallback/display purposes only.
    sub_data = load_sub_tiers()
    sub_data["tiers"] = current_tiers
    sub_data["gifted_tiers"] = gifted_tiers
    save_sub_tiers(sub_data)
    return 0.0, 0.0

def fetch_all_stats(config):
    client_id = config["client_id"]
    broadcaster_id = config["broadcaster_id"]

    token = config.get("access_token")
    if not token:
        print("ERROR: No access token. Run setup again: python twitch_stats.py --setup")
        return None

    try:
        subscribers = get_subscribers(client_id, token, broadcaster_id)
    except Exception as e:
        print(f"ERROR fetching subscribers: {e}")
        subscribers = 0

    try:
        followers = get_followers(client_id, token, broadcaster_id)
    except Exception as e:
        print(f"ERROR fetching followers: {e}")
        followers = 0

    subs_fetch_ok = True
    try:
        current_tiers, current_subs = get_sub_tiers(client_id, token, broadcaster_id)
        gifted_tiers = count_gifted_subs(current_subs)
    except Exception as e:
        subs_fetch_ok = False
        print(f"ERROR fetching sub tiers: {e}")
        current_tiers = {"1000": 0, "2000": 0, "3000": 0}
        current_subs = []
        gifted_tiers = {"1000": 0, "2000": 0, "3000": 0}

    sync_info = {
        "baseline": False,
        "earned_nongift_revenue": 0.0,
        "earned_gifted_revenue": 0.0,
    }
    if subs_fetch_ok:
        sync_info = sync_subscribers(current_subs)

    sub_data = load_sub_tiers()

    if subscribers == 0 and sub_data.get("tiers"):
        total_manual_subs = sum(sub_data["tiers"].values())
        if total_manual_subs > 0:
            subscribers = total_manual_subs

    # Helix can occasionally return a `total` that doesn't match the paged list size.
    # Prefer the larger value to avoid displaying an undercount.
    try:
        tier_sum = int(sum(int(v or 0) for v in current_tiers.values()))
        if tier_sum > 0 and int(subscribers or 0) > 0:
            subscribers = max(int(subscribers), tier_sum)
    except Exception:
        pass

    if not current_tiers["1000"] and not current_tiers["2000"] and not current_tiers["3000"]:
        if sub_data.get("tiers"):
            current_tiers = sub_data["tiers"]

    bits_data = load_bits_tracker()
    bits_today_revenue = float(bits_data.get("today_bits_revenue", 0.0) or 0.0)
    bits_total_revenue = float(bits_data.get("lifetime_bits_revenue", 0.0) or 0.0)

    # Persist tiers for fallback/display purposes only (do not use for revenue).
    try:
        tier_sum = int(sum(int(v or 0) for v in current_tiers.values()))
    except Exception:
        tier_sum = 0
    if subs_fetch_ok and tier_sum > 0:
        update_totals(current_tiers, gifted_tiers)

    # Earnings are counted from IRC/EventSub events only. The Helix subscription list
    # is used for active counts/tiers, not revenue.
    earnings_data = load_artist_earnings()
    current_month = get_current_month_key()
    monthly_artist_bits = float(earnings_data.get("monthly_bits", {}).get(current_month, 0.0) or 0.0)
    monthly_artist_subs = float(earnings_data.get("monthly_subs", {}).get(current_month, 0.0) or 0.0)
    monthly_artist_gifted = float(earnings_data.get("monthly_gifted", {}).get(current_month, 0.0) or 0.0)
    lifetime_total = float(earnings_data.get("lifetime_total", 0.0) or 0.0)

    sub_rev = load_sub_revenue()
    subs_today_total = float(sub_rev.get("today_total", 0.0) or 0.0)
    subs_lifetime_total = float(sub_rev.get("lifetime_total", 0.0) or 0.0)

    goal_data = load_sub_goal()
    sub_goal_current = int(goal_data.get("current", 0) or 0)
    sub_goal_target = int(goal_data.get("goal", 150) or 150)

    # Update and get hours streamed
    hours_data = update_hours_streamed()
    hours_stats = get_hours_streamed()

    return {
        "timestamp": datetime.now().isoformat(),
        "subscribers": subscribers,
        "followers": followers,
        "bits_today": round(bits_today_revenue, 2),
        "bits_total": round(bits_total_revenue, 2),
        "subs_today_revenue": round(subs_today_total, 2),
        "subs_total_revenue": round(subs_lifetime_total, 2),
        "sub_goal_current": sub_goal_current,
        "sub_goal_target": sub_goal_target,
        "artist_bits_monthly": round(monthly_artist_bits, 2),
        "artist_subs_monthly": round(monthly_artist_subs, 2),
        "artist_gifted_monthly": round(monthly_artist_gifted, 2),
        "artist_raise_30": round(monthly_artist_bits + monthly_artist_subs + monthly_artist_gifted, 2),
        "artist_yearly": round(lifetime_total, 2),
        "total_revenue": round(bits_total_revenue + subs_lifetime_total, 2),
        "tiers": current_tiers,
        "gifted_tiers": gifted_tiers,
        "hours_total": hours_stats["total_hours"],
        "hours_monthly": hours_stats["monthly_hours"],
        "hours_today": hours_stats["today_hours"],
        "hours_session": hours_stats["current_session_hours"]
    }

def save_to_archive(stats):
    rotate_archive_if_needed()
    archive_file = get_archive_filename()
    file_exists = os.path.exists(archive_file)

    with open(archive_file, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=ARCHIVE_FIELDS, extrasaction='ignore')
        if not file_exists:
            writer.writeheader()

        row = {k: stats.get(k, 0) for k in ARCHIVE_FIELDS}
        # Ensure dict-like columns exist.
        if not isinstance(row.get("tiers"), dict):
            row["tiers"] = stats.get("tiers") if isinstance(stats.get("tiers"), dict) else {}
        if not isinstance(row.get("gifted_tiers"), dict):
            row["gifted_tiers"] = stats.get("gifted_tiers") if isinstance(stats.get("gifted_tiers"), dict) else {}

        writer.writerow(row)

def save_latest(stats):
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp_path = LATEST_FILE + ".tmp"
    with open(tmp_path, 'w') as f:
        json.dump(stats, f, indent=2)
    os.replace(tmp_path, LATEST_FILE)

def setup_config():
    print("Twitch Stats Tracker Setup")
    print("=" * 40)
    client_id = input("Enter your Twitch Client ID: ").strip()
    client_secret = input("Enter your Twitch Client Secret: ").strip()
    username = input("Enter your Twitch Username: ").strip()

    redirect_uri = "http://localhost:3000"
    oauth_url = get_oauth_url(client_id, redirect_uri)

    print(f"\n1. Open this URL in your browser:")
    print(f"\n{oauth_url}\n")
    print("2. Authorize the app")
    print("3. COPY the access_token from the URL")
    print("   (The URL will look like: http://localhost:3000#access_token=YOURTOKEN...)")
    print("\n4. Paste the access token below:")

    access_token = input("Access Token: ").strip()

    broadcaster_id = get_user_id(client_id, access_token, username)
    if not broadcaster_id:
        print(f"Could not find user '{username}'. Check your username and try again.")
        return None

    config = {
        "client_id": client_id,
        "client_secret": client_secret,
        "broadcaster_id": broadcaster_id,
        "access_token": access_token,
        "username": username,
        "channel": username,
        "enable_irc_bits": True,
        "debug_irc": False,
        "enable_eventsub_bits": True,
        "debug_eventsub": False,
        "poll_interval_seconds": 60
    }
    save_config(config)
    print(f"\nConfig saved! Run 'python twitch_stats.py' to start tracking")
    return config

def set_initial_totals():
    print("\nSet Initial Totals (from August 2024)")
    print("=" * 40)
    print("Enter your current subscriber tiers from Twitch Creator Dashboard")
    print()
    print("Note: Bits will be pulled from API automatically")
    print()

    print("Current Subs by Tier (from Twitch Dashboard):")
    try:
        tier1 = int(input("  Tier 1 ($4.19) subs: ").strip())
        tier2 = int(input("  Tier 2 ($6.99) subs: ").strip())
        tier3 = int(input("  Tier 3 ($17.49) subs: ").strip())
    except ValueError:
        tier1 = tier2 = tier3 = 0

    total_subs_revenue = (tier1 * 4.19) + (tier2 * 6.99) + (tier3 * 17.49)

    print()
    print("Optional: enter your lifetime Bits *revenue* in USD (from Twitch analytics)")
    print("If you don't know it yet, leave blank to start from $0.00.")
    bits_lifetime_input = input("  Lifetime Bits Revenue ($): ").strip()
    try:
        lifetime_bits_revenue = float(bits_lifetime_input.replace('$', '').replace(',', '')) if bits_lifetime_input else 0.0
    except Exception:
        lifetime_bits_revenue = 0.0

    sub_data = {
        "total_subs_revenue": total_subs_revenue,
        "gifted_subs_revenue": 0,
        "nongift_subs_revenue": total_subs_revenue,
        "tiers": {
            "1000": tier1,
            "2000": tier2,
            "3000": tier3
        },
        "gifted_tiers": {"1000": 0, "2000": 0, "3000": 0}
    }
    save_sub_tiers(sub_data)

    # Seed earned subs revenue tracker from the provided baseline.
    try:
        seed = {
            "date": get_current_date_key(),
            "today_total": 0.0,
            "today_nongift": 0.0,
            "today_gifted": 0.0,
            "lifetime_total": round(float(total_subs_revenue or 0.0), 2),
            "lifetime_nongift": round(float(total_subs_revenue or 0.0), 2),
            "lifetime_gifted": 0.0,
        }
        save_sub_revenue(seed)
    except Exception:
        pass

    bits_data = load_bits_tracker()
    bits_data["lifetime_bits_count"] = int(bits_data.get("lifetime_bits_count", 0) or 0)
    # Treat this as imported lifetime bits revenue (historical seed).
    bits_data["lifetime_bits_revenue_imported"] = round(float(lifetime_bits_revenue), 2)
    bits_data["today_bits_count"] = 0
    bits_data["today_bits_revenue"] = 0.0
    bits_data["date"] = get_current_date_key()
    bits_data["lifetime_bits_revenue"] = round(
        float(bits_data.get("lifetime_bits_revenue_tracked", 0.0) or 0.0)
        + float(bits_data.get("lifetime_bits_revenue_imported", 0.0) or 0.0),
        2,
    )
    save_bits_tracker(bits_data)

    # Artist gets 30% of streamer's Twitch payout (Option B)
    artist_subs_lifetime = (tier1 * TIER_VALUES["1000"] + tier2 * TIER_VALUES["2000"] + tier3 * TIER_VALUES["3000"]) * 0.30

    earnings_data = {
        "monthly_bits": {},
        "monthly_subs": {},
        "monthly_gifted": {},
        "lifetime_bits": round(lifetime_bits_revenue * 0.30, 2),
        "lifetime_subs": round(artist_subs_lifetime, 2),
        "lifetime_gifted": 0,
        "lifetime_total": round((lifetime_bits_revenue * 0.30) + artist_subs_lifetime, 2)
    }
    save_artist_earnings(earnings_data)

    print(f"\nInitial totals set!")
    print(f"  Tier 1: {tier1} x $4.19")
    print(f"  Tier 2: {tier2} x $6.99")
    print(f"  Tier 3: {tier3} x $17.49")
    print(f"  Subs Revenue: ${total_subs_revenue:.2f}")
    print(f"  Artist Subs Lifetime: ${artist_subs_lifetime:.2f}")
    print(f"\nRun 'python twitch_stats.py' to start tracking")

def _parse_csv_currency(value):
    """Parse a currency string from CSV (handles $, commas, empty values)."""
    if not value:
        return 0.0
    try:
        return float(str(value).replace('$', '').replace(',', '').strip())
    except (ValueError, TypeError):
        return 0.0

def _parse_csv_int(value):
    """Parse an integer from CSV (handles commas, empty values)."""
    if not value:
        return 0
    try:
        return int(str(value).replace(',', '').strip())
    except (ValueError, TypeError):
        return 0

def import_csv_history(folder):
    """Import historical data from Twitch Analytics CSV exports.

    Uses Twitch's actual reported revenue columns for accuracy:
    - Sub Revenue, Prime Revenue, Gifted Subs Revenue, Bits Revenue

    Also creates archive files for dashboard display.
    Artist earnings = 30% of your Twitch payout (Option B).
    """
    print(f"\nImporting CSV history from: {folder}")
    print("=" * 40)
    print("Using Twitch's actual revenue columns for accuracy")
    print("Creating archive files for dashboard...")

    if not os.path.exists(folder):
        print(f"Folder not found: {folder}")
        return

    csv_files = glob.glob(os.path.join(folder, "*.csv"))
    if not csv_files:
        print("No CSV files found in folder")
        return

    # Reset earnings data for fresh import
    earnings_data = {
        "monthly_bits": {},
        "monthly_subs": {},
        "monthly_gifted": {},
        "lifetime_bits": 0,
        "lifetime_subs": 0,
        "lifetime_gifted": 0,
        "lifetime_total": 0
    }

    # Track totals for summary
    imported_bits_total = 0.0
    imported_subs_total = 0.0
    imported_gifted_total = 0.0
    imported_prime_total = 0.0

    # First pass: collect all data rows from all files
    # Extended format with all columns needed for archive
    all_rows = []

    print("\nFirst pass: collecting all data...")

    for csv_file in sorted(csv_files):
        filename = os.path.basename(csv_file)

        try:
            with open(csv_file, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    continue

                # Check if this file has revenue columns
                has_revenue_cols = 'Sub Revenue' in reader.fieldnames or 'Bits Revenue' in reader.fieldnames

                if not has_revenue_cols:
                    continue

                for row in reader:
                    date = row.get('Date', '').strip()
                    if not date:
                        continue

                    # Parse date to standardized format
                    date_key = None
                    for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%a %b %d %Y"]:
                        try:
                            date_key = datetime.strptime(date, fmt).strftime("%Y-%m-%d")
                            break
                        except ValueError:
                            continue

                    if not date_key:
                        continue

                    month = date_key[:7]  # YYYY-MM

                    # Read actual revenue columns from Twitch CSV
                    bits_rev = _parse_csv_currency(row.get('Bits Revenue', ''))
                    sub_rev = _parse_csv_currency(row.get('Sub Revenue', ''))
                    prime_rev = _parse_csv_currency(row.get('Prime Revenue', ''))
                    gifted_rev = _parse_csv_currency(row.get('Gifted Subs Revenue', ''))

                    # Read subscriber/follower counts for archive
                    total_paid_subs = _parse_csv_int(row.get('Total Paid Subs', ''))
                    prime_subs = _parse_csv_int(row.get('Prime Subs', ''))
                    subscribers = total_paid_subs + prime_subs

                    # Read tier breakdowns
                    tier1 = _parse_csv_int(row.get('Tier 1 subs', ''))
                    tier2 = _parse_csv_int(row.get('Tier 2 subs', ''))
                    tier3 = _parse_csv_int(row.get('Tier 3 subs', ''))

                    # Read gifted tier breakdowns
                    gifted_tier1 = _parse_csv_int(row.get('Gifted Tier 1 subs', ''))
                    gifted_tier2 = _parse_csv_int(row.get('Gifted Tier 2 subs', ''))
                    gifted_tier3 = _parse_csv_int(row.get('Gifted Tier 3 subs', ''))

                    # Followers not in revenue CSV, use 0
                    followers = 0

                    # Read minutes streamed for hours tracking
                    minutes_streamed = _parse_csv_int(row.get('Minutes Streamed', ''))

                    all_rows.append({
                        'date_key': date_key,
                        'month': month,
                        'bits_rev': bits_rev,
                        'sub_rev': sub_rev,
                        'prime_rev': prime_rev,
                        'gifted_rev': gifted_rev,
                        'subscribers': subscribers,
                        'followers': followers,
                        'tier1': tier1,
                        'tier2': tier2,
                        'tier3': tier3,
                        'gifted_tier1': gifted_tier1,
                        'gifted_tier2': gifted_tier2,
                        'gifted_tier3': gifted_tier3,
                        'minutes_streamed': minutes_streamed,
                        'filename': filename
                    })

        except Exception as e:
            print(f"  Error reading {filename}: {e}")

    if not all_rows:
        print("No valid data found in any files")
        return

    # Sort all rows by date and deduplicate
    all_rows.sort(key=lambda x: x['date_key'])

    print(f"\nSecond pass: processing {len(all_rows)} records chronologically...")
    print("=" * 40)

    # Ensure archive directory exists
    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    processed_dates = set()
    running_bits_total = 0.0
    running_subs_total = 0.0
    archive_count = 0

    # Hours tracking
    hours_data = {
        "total_hours": 0.0,
        "monthly_hours": {},
        "daily_hours": {}
    }

    for row in all_rows:
        date_key = row['date_key']
        month = row['month']
        bits_rev = row['bits_rev']
        sub_rev = row['sub_rev']
        prime_rev = row['prime_rev']
        gifted_rev = row['gifted_rev']

        # Skip if we already processed this exact date
        if date_key in processed_dates:
            continue
        processed_dates.add(date_key)

        # Calculate daily revenue
        subs_today = sub_rev + prime_rev + gifted_rev

        # Update running totals
        running_bits_total += bits_rev
        running_subs_total += subs_today

        # Calculate artist earnings (30% of your Twitch payout)
        artist_bits = bits_rev * 0.30
        artist_subs = (sub_rev + prime_rev) * 0.30
        artist_gifted = gifted_rev * 0.30

        # Accumulate monthly totals for artist earnings
        earnings_data["monthly_bits"][month] = round(
            earnings_data["monthly_bits"].get(month, 0) + artist_bits, 2)
        earnings_data["monthly_subs"][month] = round(
            earnings_data["monthly_subs"].get(month, 0) + artist_subs, 2)
        earnings_data["monthly_gifted"][month] = round(
            earnings_data["monthly_gifted"].get(month, 0) + artist_gifted, 2)

        # Accumulate import totals for summary
        imported_bits_total += bits_rev
        imported_subs_total += sub_rev
        imported_prime_total += prime_rev
        imported_gifted_total += gifted_rev

        # Calculate artist monthly/yearly for archive
        artist_bits_monthly = earnings_data["monthly_bits"].get(month, 0)
        artist_subs_monthly = earnings_data["monthly_subs"].get(month, 0)
        artist_gifted_monthly = earnings_data["monthly_gifted"].get(month, 0)
        artist_raise_30 = artist_bits_monthly + artist_subs_monthly + artist_gifted_monthly
        artist_yearly = sum(earnings_data["monthly_bits"].values()) + \
                       sum(earnings_data["monthly_subs"].values()) + \
                       sum(earnings_data["monthly_gifted"].values())

        # Create archive file for this day
        archive_stats = {
            "timestamp": f"{date_key}T23:59:59",
            "subscribers": row['subscribers'],
            "followers": row['followers'],
            "bits_today": round(bits_rev, 2),
            "bits_total": round(running_bits_total, 2),
            "subs_today_revenue": round(subs_today, 2),
            "subs_total_revenue": round(running_subs_total, 2),
            "artist_bits_monthly": round(artist_bits_monthly, 2),
            "artist_subs_monthly": round(artist_subs_monthly, 2),
            "artist_gifted_monthly": round(artist_gifted_monthly, 2),
            "artist_raise_30": round(artist_raise_30, 2),
            "artist_yearly": round(artist_yearly, 2),
            "total_revenue": round(running_bits_total + running_subs_total, 2),
            "tiers": {"1000": row['tier1'], "2000": row['tier2'], "3000": row['tier3']},
            "gifted_tiers": {"1000": row['gifted_tier1'], "2000": row['gifted_tier2'], "3000": row['gifted_tier3']}
        }

        # Write archive file
        archive_file = os.path.join(ARCHIVE_DIR, f"stats_archive_{date_key}.csv")
        with open(archive_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=ARCHIVE_FIELDS, extrasaction='ignore')
            writer.writeheader()
            archive_row = {k: archive_stats.get(k, 0) for k in ARCHIVE_FIELDS}
            archive_row["tiers"] = archive_stats["tiers"]
            archive_row["gifted_tiers"] = archive_stats["gifted_tiers"]
            writer.writerow(archive_row)
        archive_count += 1

        # Accumulate hours from minutes streamed
        minutes_streamed = row.get('minutes_streamed', 0)
        if minutes_streamed > 0:
            hours_today = minutes_streamed / 60.0
            hours_data["total_hours"] += hours_today
            hours_data["daily_hours"][date_key] = round(hours_today, 2)
            hours_data["monthly_hours"][month] = round(
                hours_data["monthly_hours"].get(month, 0) + hours_today, 2
            )

        # Print daily summary (only if there's activity)
        total_day_rev = bits_rev + sub_rev + prime_rev + gifted_rev
        if total_day_rev > 0:
            print(f"  {date_key}: Bits=${bits_rev:.2f} Subs=${sub_rev:.2f} Prime=${prime_rev:.2f} Gifted=${gifted_rev:.2f} | Artist=${(artist_bits + artist_subs + artist_gifted):.2f}")

    # Calculate lifetime totals
    lifetime_bits = sum(earnings_data["monthly_bits"].values())
    lifetime_subs = sum(earnings_data["monthly_subs"].values())
    lifetime_gifted = sum(earnings_data["monthly_gifted"].values())

    earnings_data["lifetime_bits"] = round(lifetime_bits, 2)
    earnings_data["lifetime_subs"] = round(lifetime_subs, 2)
    earnings_data["lifetime_gifted"] = round(lifetime_gifted, 2)
    earnings_data["lifetime_total"] = round(lifetime_bits + lifetime_subs + lifetime_gifted, 2)

    save_artist_earnings(earnings_data)

    # Seed bits lifetime totals from imported CSV revenue
    try:
        bits_data = load_bits_tracker()
        bits_data["lifetime_bits_revenue_imported"] = round(imported_bits_total, 2)
        bits_data["lifetime_bits_revenue"] = round(
            float(bits_data.get("lifetime_bits_revenue_tracked", 0.0) or 0.0)
            + imported_bits_total,
            2,
        )
        save_bits_tracker(bits_data)
    except Exception as e:
        print(f"WARNING: Failed to seed bits totals: {e}")

    # Seed sub revenue lifetime totals from imported CSV revenue
    try:
        total_sub_revenue = imported_subs_total + imported_prime_total + imported_gifted_total
        sub_rev_data = {
            "date": get_current_date_key(),
            "today_total": 0.0,
            "today_nongift": 0.0,
            "today_gifted": 0.0,
            "lifetime_total": round(total_sub_revenue, 2),
            "lifetime_nongift": round(imported_subs_total + imported_prime_total, 2),
            "lifetime_gifted": round(imported_gifted_total, 2),
        }
        save_sub_revenue(sub_rev_data)
    except Exception as e:
        print(f"WARNING: Failed to seed sub revenue totals: {e}")

    # Save hours streamed data from import
    try:
        hours_data["total_hours"] = round(hours_data["total_hours"], 2)
        save_hours_streamed(hours_data)
        print(f"\n  HOURS STREAMED (from CSV):")
        print(f"    Total Hours: {hours_data['total_hours']:.1f}")
    except Exception as e:
        print(f"WARNING: Failed to save hours data: {e}")

    print(f"\n" + "=" * 40)
    print("Import complete! (Using Twitch's actual revenue data)")
    print(f"\n  DASHBOARD ARCHIVE:")
    print(f"    Created {archive_count} daily archive files")
    print(f"    Location: {ARCHIVE_DIR}/")
    print(f"\n  YOUR REVENUE (100%):")
    print(f"    Bits:   ${imported_bits_total:,.2f}")
    print(f"    Subs:   ${imported_subs_total:,.2f}")
    print(f"    Prime:  ${imported_prime_total:,.2f}")
    print(f"    Gifted: ${imported_gifted_total:,.2f}")
    print(f"    TOTAL:  ${(imported_bits_total + imported_subs_total + imported_prime_total + imported_gifted_total):,.2f}")
    print(f"\n  ARTIST EARNINGS (30% of your payout):")
    print(f"    From Bits:   ${lifetime_bits:,.2f}")
    print(f"    From Subs:   ${lifetime_subs:,.2f}")
    print(f"    From Gifted: ${lifetime_gifted:,.2f}")
    print(f"    TOTAL:       ${earnings_data['lifetime_total']:,.2f}")
    print(f"\nDashboard: http://localhost:3001/dashboard.html")
    print(f"Run 'python twitch_stats.py' to start tracking")


def backfill_archives_only(folder):
    """Backfill dashboard archive files from Twitch CSV exports WITHOUT modifying live data.

    This creates archive files for the dashboard to display historical charts,
    but does NOT touch:
    - artist_earnings.json
    - bits_tracker.json
    - sub_revenue.json
    - subscribers.json
    - latest_stats.json

    Safe to run without affecting your live tracking data.
    """
    print(f"\nBackfilling dashboard archives from: {folder}")
    print("=" * 40)
    print("NOTE: This only creates archive files for dashboard display.")
    print("      Your live tracking data will NOT be modified.")

    if not os.path.exists(folder):
        print(f"Folder not found: {folder}")
        return

    csv_files = glob.glob(os.path.join(folder, "*.csv"))
    if not csv_files:
        print("No CSV files found in folder")
        return

    # Collect all data rows
    all_rows = []

    print("\nReading CSV files...")

    for csv_file in sorted(csv_files):
        filename = os.path.basename(csv_file)

        try:
            with open(csv_file, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    continue

                has_revenue_cols = 'Sub Revenue' in reader.fieldnames or 'Bits Revenue' in reader.fieldnames
                if not has_revenue_cols:
                    continue

                for row in reader:
                    date = row.get('Date', '').strip()
                    if not date:
                        continue

                    date_key = None
                    for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%a %b %d %Y"]:
                        try:
                            date_key = datetime.strptime(date, fmt).strftime("%Y-%m-%d")
                            break
                        except ValueError:
                            continue

                    if not date_key:
                        continue

                    month = date_key[:7]

                    bits_rev = _parse_csv_currency(row.get('Bits Revenue', ''))
                    sub_rev = _parse_csv_currency(row.get('Sub Revenue', ''))
                    prime_rev = _parse_csv_currency(row.get('Prime Revenue', ''))
                    gifted_rev = _parse_csv_currency(row.get('Gifted Subs Revenue', ''))

                    total_paid_subs = _parse_csv_int(row.get('Total Paid Subs', ''))
                    prime_subs = _parse_csv_int(row.get('Prime Subs', ''))
                    subscribers = total_paid_subs + prime_subs

                    tier1 = _parse_csv_int(row.get('Tier 1 subs', ''))
                    tier2 = _parse_csv_int(row.get('Tier 2 subs', ''))
                    tier3 = _parse_csv_int(row.get('Tier 3 subs', ''))

                    gifted_tier1 = _parse_csv_int(row.get('Gifted Tier 1 subs', ''))
                    gifted_tier2 = _parse_csv_int(row.get('Gifted Tier 2 subs', ''))
                    gifted_tier3 = _parse_csv_int(row.get('Gifted Tier 3 subs', ''))

                    all_rows.append({
                        'date_key': date_key,
                        'month': month,
                        'bits_rev': bits_rev,
                        'sub_rev': sub_rev,
                        'prime_rev': prime_rev,
                        'gifted_rev': gifted_rev,
                        'subscribers': subscribers,
                        'tier1': tier1,
                        'tier2': tier2,
                        'tier3': tier3,
                        'gifted_tier1': gifted_tier1,
                        'gifted_tier2': gifted_tier2,
                        'gifted_tier3': gifted_tier3,
                    })

        except Exception as e:
            print(f"  Error reading {filename}: {e}")

    if not all_rows:
        print("No valid data found in any files")
        return

    all_rows.sort(key=lambda x: x['date_key'])

    print(f"\nCreating {len(all_rows)} archive files...")
    print("=" * 40)

    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    # Track monthly artist earnings for archive (not saved to JSON)
    monthly_bits = {}
    monthly_subs = {}
    monthly_gifted = {}

    processed_dates = set()
    running_bits_total = 0.0
    running_subs_total = 0.0
    archive_count = 0
    skipped_count = 0

    for row in all_rows:
        date_key = row['date_key']
        month = row['month']

        if date_key in processed_dates:
            continue
        processed_dates.add(date_key)

        # Check if archive already exists (don't overwrite live data)
        archive_file = os.path.join(ARCHIVE_DIR, f"stats_archive_{date_key}.csv")
        if os.path.exists(archive_file):
            skipped_count += 1
            continue

        bits_rev = row['bits_rev']
        sub_rev = row['sub_rev']
        prime_rev = row['prime_rev']
        gifted_rev = row['gifted_rev']

        subs_today = sub_rev + prime_rev + gifted_rev
        running_bits_total += bits_rev
        running_subs_total += subs_today

        # Calculate artist earnings for this archive only
        artist_bits = bits_rev * 0.30
        artist_subs = (sub_rev + prime_rev) * 0.30
        artist_gifted = gifted_rev * 0.30

        monthly_bits[month] = monthly_bits.get(month, 0) + artist_bits
        monthly_subs[month] = monthly_subs.get(month, 0) + artist_subs
        monthly_gifted[month] = monthly_gifted.get(month, 0) + artist_gifted

        artist_bits_monthly = monthly_bits.get(month, 0)
        artist_subs_monthly = monthly_subs.get(month, 0)
        artist_gifted_monthly = monthly_gifted.get(month, 0)
        artist_raise_30 = artist_bits_monthly + artist_subs_monthly + artist_gifted_monthly
        artist_yearly = sum(monthly_bits.values()) + sum(monthly_subs.values()) + sum(monthly_gifted.values())

        archive_stats = {
            "timestamp": f"{date_key}T23:59:59",
            "subscribers": row['subscribers'],
            "followers": 0,
            "bits_today": round(bits_rev, 2),
            "bits_total": round(running_bits_total, 2),
            "subs_today_revenue": round(subs_today, 2),
            "subs_total_revenue": round(running_subs_total, 2),
            "artist_bits_monthly": round(artist_bits_monthly, 2),
            "artist_subs_monthly": round(artist_subs_monthly, 2),
            "artist_gifted_monthly": round(artist_gifted_monthly, 2),
            "artist_raise_30": round(artist_raise_30, 2),
            "artist_yearly": round(artist_yearly, 2),
            "total_revenue": round(running_bits_total + running_subs_total, 2),
            "tiers": {"1000": row['tier1'], "2000": row['tier2'], "3000": row['tier3']},
            "gifted_tiers": {"1000": row['gifted_tier1'], "2000": row['gifted_tier2'], "3000": row['gifted_tier3']}
        }

        with open(archive_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=ARCHIVE_FIELDS, extrasaction='ignore')
            writer.writeheader()
            archive_row = {k: archive_stats.get(k, 0) for k in ARCHIVE_FIELDS}
            archive_row["tiers"] = archive_stats["tiers"]
            archive_row["gifted_tiers"] = archive_stats["gifted_tiers"]
            writer.writerow(archive_row)

        archive_count += 1

        total_day_rev = bits_rev + sub_rev + prime_rev + gifted_rev
        if total_day_rev > 0:
            print(f"  {date_key}: Bits=${bits_rev:.2f} Subs=${sub_rev + prime_rev:.2f} Gifted=${gifted_rev:.2f}")

    # Generate consolidated JSON for fast dashboard loading
    generate_dashboard_json()

    print(f"\n" + "=" * 40)
    print("Backfill complete!")
    print(f"  Created: {archive_count} new archive files")
    print(f"  Skipped: {skipped_count} (already exist - preserving live data)")
    print(f"  Location: {ARCHIVE_DIR}/")
    print(f"\nYour live tracking data was NOT modified.")
    print(f"\nDashboard: http://localhost:3001/dashboard.html")
    print(f"Run 'python twitch_stats.py' to start tracking")


def generate_dashboard_json():
    """Generate a consolidated JSON file from all archive CSVs for fast dashboard loading."""
    print("\nGenerating dashboard_data.json...")

    archive_files = glob.glob(os.path.join(ARCHIVE_DIR, "stats_archive_*.csv"))
    if not archive_files:
        print("  No archive files found")
        return

    all_data = []

    for archive_file in sorted(archive_files):
        filename = os.path.basename(archive_file)
        date_str = filename.replace("stats_archive_", "").replace(".csv", "")

        try:
            with open(archive_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    stats = {"date": date_str}
                    for key, value in row.items():
                        if key in ('tiers', 'gifted_tiers'):
                            # Parse dict-like string
                            try:
                                stats[key] = eval(value) if value else {}
                            except:
                                stats[key] = {}
                        elif key == 'timestamp':
                            stats[key] = value
                        else:
                            try:
                                stats[key] = float(value) if value else 0
                            except:
                                stats[key] = 0
                    all_data.append(stats)
                    break  # Only take first data row (header + 1 row per file)
        except Exception as e:
            print(f"  Warning: Could not read {filename}: {e}")

    # Sort by date
    all_data.sort(key=lambda x: x.get('date', ''))

    # Write consolidated JSON
    output_file = os.path.join(DATA_DIR, "dashboard_data.json")
    tmp_path = output_file + ".tmp"
    with open(tmp_path, 'w') as f:
        json.dump(all_data, f)
    os.replace(tmp_path, output_file)

    print(f"  Created: {output_file} ({len(all_data)} days)")


class StatsHandler(http.server.SimpleHTTPRequestHandler):
    # Files/patterns that must never be served over HTTP.
    _BLOCKED_PATHS = {
        "/config.json",
        "/.env",
        "/agent.md",
    }
    _BLOCKED_PREFIXES = (
        "/__pycache__/",
        "/.git/",
    )
    _BLOCKED_EXTENSIONS = (
        ".py",
        ".pyc",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=os.path.dirname(os.path.abspath(__file__)), **kwargs)

    def do_GET(self):
        """Block access to sensitive files (config, source code, etc.)."""
        from urllib.parse import urlparse
        path = urlparse(self.path).path
        # Normalize to prevent path traversal via double slashes, etc.
        path = os.path.normpath(path).replace("\\", "/")
        if not path.startswith("/"):
            path = "/" + path

        # API endpoints
        if path == '/api/hours':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            hours_stats = get_hours_streamed()
            self.wfile.write(json.dumps(hours_stats).encode())
            return

        if path == '/api/stats':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            try:
                with open(LATEST_FILE, 'r') as f:
                    stats = json.load(f)
            except Exception:
                stats = {}
            self.wfile.write(json.dumps(stats).encode())
            return

        if (
            path in self._BLOCKED_PATHS
            or path.startswith(self._BLOCKED_PREFIXES)
            or path.endswith(self._BLOCKED_EXTENSIONS)
        ):
            self.send_response(403)
            self.end_headers()
            self.wfile.write(b"Forbidden")
            return

        super().do_GET()

    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.send_header('Cache-Control', 'no-cache')
        super().end_headers()
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()
    
    def do_POST(self):
        parsed_path = urlparse(self.path)

        if parsed_path.path == '/api/add-bits':
            # Manual bits import from admin page
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length > 0:
                post_data = self.rfile.read(content_length)

                try:
                    data = json.loads(post_data.decode('utf-8'))
                    bits = int(data.get('bits', 0))
                    username = str(data.get('username', 'Manual')).strip()

                    if bits <= 0:
                        raise ValueError("Bits must be positive")

                    # Record the bits using existing function
                    record_cheer(bits)

                    # Send success response
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()

                    # Get updated totals
                    bits_data = load_bits_tracker()

                    self.wfile.write(json.dumps({
                        'success': True,
                        'message': f'Added {bits} bits from {username}',
                        'bits_today': bits_data.get('today_bits_revenue', 0),
                        'bits_total': bits_data.get('lifetime_bits_revenue', 0)
                    }).encode())

                    print(f"  + Manual bits: {bits} from {username}")

                except Exception as e:
                    self.send_response(400)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({
                        'success': False,
                        'error': str(e)
                    }).encode())
            else:
                self.send_response(400)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({
                    'success': False,
                    'error': 'No data provided'
                }).encode())

        elif parsed_path.path == '/api/save-branding':
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length > 0:
                post_data = self.rfile.read(content_length)

                try:
                    branding_config = json.loads(post_data.decode('utf-8'))

                    # Create backup of existing branding.json before saving
                    try:
                        os.makedirs(DATA_DIR, exist_ok=True)
                        if os.path.exists(BRANDING_CONFIG_FILE):
                            backup_file = BRANDING_CONFIG_FILE + f".backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                            import shutil
                            shutil.copy2(BRANDING_CONFIG_FILE, backup_file)
                            # Keep only last 5 backups
                            backup_pattern = BRANDING_CONFIG_FILE + ".backup.*"
                            backups = sorted(glob.glob(backup_pattern), reverse=True)
                            for old_backup in backups[5:]:
                                try:
                                    os.remove(old_backup)
                                except Exception:
                                    pass
                    except Exception as e:
                        print(f"Warning: Could not create branding backup: {e}")

                    # Generate the branding-setup.js content
                    js_content = generate_branding_js(branding_config)

                    # Write to file
                    with open('branding-setup.js', 'w') as f:
                        f.write(js_content)

                    # Also persist the raw config for debugging/backup.
                    try:
                        tmp_path = BRANDING_CONFIG_FILE + ".tmp"
                        with open(tmp_path, 'w') as f:
                            json.dump(branding_config, f, indent=2)
                        os.replace(tmp_path, BRANDING_CONFIG_FILE)
                    except Exception:
                        pass

                    # Send success response
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({
                        'success': True,
                        'message': 'Branding saved successfully!'
                    }).encode())

                    print(f"Branding updated: {branding_config.get('fontFamily', 'Unknown')} font")

                except Exception as e:
                    self.send_response(500)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({
                        'success': False,
                        'error': str(e)
                    }).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass

def _js_string_escape(value):
    """Escape a value for safe interpolation inside a JS double-quoted string."""
    s = str(value)
    s = s.replace("\\", "\\\\")
    s = s.replace('"', '\\"')
    s = s.replace("'", "\\'")
    s = s.replace("<", "\\x3c")
    s = s.replace(">", "\\x3e")
    s = s.replace("\n", "\\n")
    s = s.replace("\r", "\\r")
    return s

def _gen_widget_js(w, defaults):
    """Generate JS object literal for a single widget config."""
    _e = _js_string_escape
    colors = w.get('colors', {})
    border = w.get('border', {})
    bg = w.get('background', {})
    padding = w.get('padding', {})
    return f"""{{
            enabled: {str(w.get('enabled', defaults.get('enabled', True))).lower()},
            fontSize: {int(w.get('fontSize', defaults['fontSize']))},
            labelFontSize: {int(w.get('labelFontSize', 14))},
            labelGap: {int(w.get('labelGap', 5))},
            colors: {{
                text: "{_e(colors.get('text', defaults['text']))}",
                label: "{_e(colors.get('label', '#aaaaaa'))}",
                border: "{_e(colors.get('border', defaults['border']))}",
                background: "{_e(colors.get('background', 'rgba(0,0,0,0.3)'))}"
            }},
            border: {{
                width: {int(border.get('width', 2))},
                style: "{_e(border.get('style', 'solid'))}",
                radius: {int(border.get('radius', 0))}
            }},
            background: {{
                enabled: {str(bg.get('enabled', False)).lower()},
                color: "{_e(bg.get('color', 'rgba(0,0,0,0.3)'))}",
                opacity: {int(bg.get('opacity', 30))}
            }},
            padding: {{
                vertical: {int(padding.get('vertical', 8))},
                horizontal: {int(padding.get('horizontal', 15))}
            }},
            textShadow: "{_e(w.get('textShadow', '2px 2px 4px rgba(0,0,0,0.8)'))}"
        }}"""


def generate_branding_js(config):
    """Generate the modular branding-setup.js file content from config"""
    import json as _json
    _e = _js_string_escape
    widgets = config.get('widgets', {})

    widget_defaults = {
        'followers': {'fontSize': 48, 'text': '#9146ff', 'border': '#9146ff'},
        'subgoal':   {'fontSize': 42, 'text': '#00d2d3', 'border': '#00d2d3'},
        'bits':      {'fontSize': 42, 'text': '#2ed573', 'border': '#2ed573'},
        'hours':     {'fontSize': 42, 'text': '#00bcd4', 'border': '#00bcd4'},
        'monthly':   {'fontSize': 42, 'text': '#ff6b6b', 'border': '#ff6b6b'},
        'lifetime':  {'fontSize': 36, 'text': '#ffd700', 'border': '#ffd700'},
    }

    widget_order = config.get('widgetOrder', ['followers', 'subgoal', 'bits', 'hours', 'monthly', 'lifetime'])
    widget_groups = config.get('widgetGroups', [])

    widget_blocks = []
    for name in ['followers', 'subgoal', 'bits', 'hours', 'monthly', 'lifetime']:
        w = widgets.get(name, {})
        d = widget_defaults[name]
        widget_blocks.append(f"        {name}: {_gen_widget_js(w, d)}")

    widgets_js = ',\n'.join(widget_blocks)

    return f'''// Central Configuration for All Widgets
// Generated by Branding Settings
// Modular - Each widget has independent styling

const DEFAULT_CONFIG = {{
    fontFamily: "{_e(config.get('fontFamily', 'Bebas Neue'))}",
    googleFont: {str(config.get('googleFont', True)).lower()},
    fontWeight: "bold",
    textShadow: "2px 2px 4px rgba(0,0,0,0.8)",

    layout: {{
        gap: {int(config.get('layout', {}).get('gap', 5))}
    }},

    widgetOrder: {_json.dumps(widget_order)},

    widgetGroups: {_json.dumps(widget_groups)},

    widgets: {{
{widgets_js}
    }}
}};

let WIDGET_CONFIG = JSON.parse(JSON.stringify(DEFAULT_CONFIG));

function loadBrandingConfig() {{
    try {{
        const stored = localStorage.getItem('brandingConfig');
        if (stored) {{
            const parsed = JSON.parse(stored);
            WIDGET_CONFIG = deepMerge(DEFAULT_CONFIG, parsed);
        }}
    }} catch (e) {{
        WIDGET_CONFIG = JSON.parse(JSON.stringify(DEFAULT_CONFIG));
    }}
}}

function deepMerge(defaults, overrides) {{
    const result = JSON.parse(JSON.stringify(defaults));
    for (const key in overrides) {{
        if (overrides.hasOwnProperty(key)) {{
            if (typeof overrides[key] === 'object' && overrides[key] !== null && !Array.isArray(overrides[key])) {{
                result[key] = deepMerge(result[key] || {{}}, overrides[key]);
            }} else {{
                result[key] = overrides[key];
            }}
        }}
    }}
    return result;
}}

function loadGoogleFont(fontName) {{
    if (WIDGET_CONFIG.googleFont && fontName) {{
        const encodedFont = fontName.replace(/ /g, '+');
        const linkId = 'google-font-dynamic';
        let link = document.getElementById(linkId);
        if (!link) {{
            link = document.createElement('link');
            link.id = linkId;
            link.rel = 'stylesheet';
            document.head.appendChild(link);
        }}
        link.href = 'https://fonts.googleapis.com/css2?family=' + encodedFont + ':wght@400;700&display=swap';
    }}
}}

function hexToRgba(hex, opacity) {{
    hex = hex.replace('#', '');
    if (hex.length === 3) {{
        hex = hex.split('').map(c => c + c).join('');
    }}
    const r = parseInt(hex.substring(0, 2), 16);
    const g = parseInt(hex.substring(2, 4), 16);
    const b = parseInt(hex.substring(4, 6), 16);
    return 'rgba(' + r + ', ' + g + ', ' + b + ', ' + (opacity / 100) + ')';
}}

function applyWidgetStyles() {{
    loadBrandingConfig();
    loadGoogleFont(WIDGET_CONFIG.fontFamily);
    
    const root = document.documentElement;
    
    root.style.setProperty('--font-family', WIDGET_CONFIG.fontFamily);
    root.style.setProperty('--font-weight', WIDGET_CONFIG.fontWeight);
    root.style.setProperty('--text-shadow', WIDGET_CONFIG.textShadow);
    root.style.setProperty('--gap', WIDGET_CONFIG.layout.gap + 'px');
    
    const widgets = ['followers', 'subgoal', 'bits', 'hours', 'monthly', 'lifetime'];

    widgets.forEach(widget => {{
        const config = WIDGET_CONFIG.widgets[widget];
        if (!config) return;

        const prefix = '--widget-' + widget;

        root.style.setProperty(prefix + '-font-size', config.fontSize + 'px');
        root.style.setProperty(prefix + '-label-font-size', config.labelFontSize + 'px');
        root.style.setProperty(prefix + '-label-gap', (config.labelGap !== undefined ? config.labelGap : 5) + 'px');
        root.style.setProperty(prefix + '-text-color', config.colors.text);
        root.style.setProperty(prefix + '-label-color', config.colors.label);
        root.style.setProperty(prefix + '-border-color', config.colors.border);

        if (config.background && config.background.enabled) {{
            const bgColor = config.background.color || '#000000';
            const opacity = config.background.opacity === undefined || config.background.opacity === null ? 30 : config.background.opacity;
            let hex = bgColor;
            if (bgColor.startsWith('rgba')) {{
                const match = bgColor.match(/rgba?\\((\\d+),\\s*(\\d+),\\s*(\\d+)/);
                if (match) {{
                    const r = parseInt(match[1]).toString(16).padStart(2, '0');
                    const g = parseInt(match[2]).toString(16).padStart(2, '0');
                    const b = parseInt(match[3]).toString(16).padStart(2, '0');
                    hex = '#' + r + g + b;
                }}
            }}
            const rgba = hexToRgba(hex, opacity);
            root.style.setProperty(prefix + '-bg-color', rgba);
        }} else {{
            root.style.setProperty(prefix + '-bg-color', 'transparent');
        }}

        root.style.setProperty(prefix + '-border-width', (config.border ? config.border.width : 2) + 'px');
        root.style.setProperty(prefix + '-border-style', config.border ? config.border.style : 'solid');
        root.style.setProperty(prefix + '-border-radius', (config.border ? config.border.radius : 0) + 'px');
        root.style.setProperty(prefix + '-padding', config.padding
            ? (config.padding.vertical + 'px ' + config.padding.horizontal + 'px')
            : '8px 15px');

        root.style.setProperty(prefix + '-text-shadow', config.textShadow || WIDGET_CONFIG.textShadow || '2px 2px 4px rgba(0,0,0,0.8)');
    }});

    // Compatibility: older templates use these generic variables.
    // Map them to the per-widget config so branding still has effect.
    try {{
        const f = WIDGET_CONFIG.widgets.followers;
        const m = WIDGET_CONFIG.widgets.monthly;
        const l = WIDGET_CONFIG.widgets.lifetime;
        if (f && f.colors) {{
            root.style.setProperty('--color-primary', f.colors.text);
            root.style.setProperty('--color-label', f.colors.label);
            root.style.setProperty('--bg-color', f.background && f.background.enabled ? (root.style.getPropertyValue('--widget-followers-bg-color') || f.colors.background || 'transparent') : 'transparent');
            root.style.setProperty('--border-width', (f.border && f.border.width !== undefined ? f.border.width : 0) + 'px');
            root.style.setProperty('--border-style', (f.border && f.border.style) ? f.border.style : 'solid');
            root.style.setProperty('--border-radius', (f.border && f.border.radius !== undefined ? f.border.radius : 0) + 'px');
            root.style.setProperty('--padding', (f.padding ? (f.padding.vertical + 'px ' + f.padding.horizontal + 'px') : '0'));
        }}
        if (m && m.colors) root.style.setProperty('--color-monthly', m.colors.text);
        if (l && l.colors) root.style.setProperty('--color-lifetime', l.colors.text);
    }} catch (e) {{}}
}}

function getWidgetOrder() {{
    return WIDGET_CONFIG.widgetOrder || ['followers', 'subgoal', 'bits', 'hours', 'monthly', 'lifetime'];
}}

function isWidgetEnabled(widget) {{
    return WIDGET_CONFIG.widgets[widget]?.enabled !== false;
}}

document.addEventListener('DOMContentLoaded', applyWidgetStyles);
'''

class ReusableTCPServer(socketserver.TCPServer):
    """TCPServer with SO_REUSEADDR to avoid 'Address already in use' on restart."""
    allow_reuse_address = True

def start_server(port=3001):
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    with ReusableTCPServer(("", port), StatsHandler) as httpd:
        print(f"Stats Overlay: http://localhost:{port}/obs_overlay.html")
        print(f"Branding Editor: http://localhost:{port}/branding.html")
        print(f"Sidebar2: http://localhost:{port}/sidebar2.html")
        httpd.serve_forever()

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        if sys.argv[1] == "--setup":
            setup_config()
        elif sys.argv[1] == "--server":
            port = int(sys.argv[2]) if len(sys.argv) > 2 else 3001
            print(f"Stats Overlay: http://localhost:{port}/obs_overlay.html")
            print(f"Branding Editor: http://localhost:{port}/branding.html")
            print(f"Sidebar2: http://localhost:{port}/sidebar2.html")
            start_server(port)
        elif sys.argv[1] == "--set-totals":
            config = load_config()
            if not config:
                print("Run --setup first")
                sys.exit(1)
            set_initial_totals()
        elif sys.argv[1] == "--import-csvs":
            if len(sys.argv) < 3:
                print("Usage: python twitch_stats.py --import-csvs <folder>")
                print("\nImports Twitch Analytics CSV exports and updates ALL data files.")
                print("Use --backfill-archives instead to preserve live tracking data.")
                sys.exit(1)
            import_csv_history(sys.argv[2])
        elif sys.argv[1] == "--backfill-archives":
            if len(sys.argv) < 3:
                print("Usage: python twitch_stats.py --backfill-archives <folder>")
                print("\nCreates dashboard archive files from Twitch CSV exports")
                print("WITHOUT modifying your live tracking data.")
                print("\nThis is safe to run - it only adds missing archive files.")
                sys.exit(1)
            backfill_archives_only(sys.argv[2])
        elif sys.argv[1] == "--simulate-cheer":
            if len(sys.argv) < 3:
                print("Usage: python twitch_stats.py --simulate-cheer <bits>")
                sys.exit(1)
            bits = sys.argv[2]
            record_cheer(bits)
            bits_data = load_bits_tracker()
            print(f"Bits Totals: ${bits_data.get('today_bits_revenue', 0.0):.2f} today, ${bits_data.get('lifetime_bits_revenue', 0.0):.2f} lifetime")
        elif sys.argv[1] == "--eventsub-list":
            config = load_config()
            if not config:
                print("Run --setup first")
                sys.exit(1)
            list_eventsub_subscriptions(config)
        elif sys.argv[1] == "--eventsub-delete":
            if len(sys.argv) < 3:
                print("Usage: python twitch_stats.py --eventsub-delete <subscription_id>")
                sys.exit(1)
            config = load_config()
            if not config:
                print("Run --setup first")
                sys.exit(1)
            delete_eventsub_subscription(config, sys.argv[2])
        else:
            print("Usage: python twitch_stats.py [options]")
            print("\nOptions:")
            print("  --setup                    Configure Twitch API credentials")
            print("  --server <port>            Web server only (no Twitch API)")
            print("  --set-totals               Set initial subscriber counts")
            print("  --import-csvs <folder>     Import CSVs and update ALL data files")
            print("  --backfill-archives <folder>  Create dashboard archives only (safe)")
            print("  --simulate-cheer <bits>    Test: add bits revenue locally")
            print("  --eventsub-list            List EventSub subscriptions")
            print("  --eventsub-delete <id>     Delete an EventSub subscription")
            print("\nExamples:")
            print("  python twitch_stats.py                    # Start full tracker")
            print("  python twitch_stats.py --backfill-archives data/import")
            print("                                            # Backfill dashboard only")
        sys.exit(0)

    config = load_config()
    if not config:
        config = setup_config()
    if not config:
        print("Setup failed. Please run 'python twitch_stats.py --setup' again.")
        sys.exit(1)

    # Optional per-channel overrides.
    try:
        ONE_TAP_GIFT_BITS_COST = int(config.get("one_tap_gift_bits_cost", ONE_TAP_GIFT_BITS_COST))
    except Exception:
        pass

    print("\n" + "=" * 50)
    print("Twitch Stats Tracker")
    print("=" * 50)
    print(f"Stats Overlay: http://localhost:3001/obs_overlay.html")
    print(f"Branding Editor: http://localhost:3001/branding.html")
    print(f"Dashboard: http://localhost:3001/dashboard.html")
    print(f"Admin: http://localhost:3001/admin_stats.html")
    print(f"Data archive: {ARCHIVE_DIR}/stats_archive_YYYY-MM-DD.csv")
    print(f"Retention: Last {ARCHIVE_RETENTION_DAYS} days")
    print("Press Ctrl+C to stop\n")

    # Regenerate dashboard JSON on startup
    try:
        generate_dashboard_json()
    except Exception as e:
        print(f"Warning: Could not generate dashboard JSON: {e}")

    # Start stream session for hours tracking
    start_stream_session()

    server_thread = threading.Thread(target=start_server, daemon=True)
    server_thread.start()

    if config.get("enable_irc_bits", True):
        start_bits_irc_listener(config)

    # Start EventSub WebSocket listener to catch small cheermotes (1-99 bits)
    # that don't appear in IRC chat. Requires: pip install websocket-client
    if config.get("enable_eventsub_bits", True):
        start_eventsub_websocket_listener(config)

    # Start keyboard listener for manual entry (cross-platform)
    def handle_hours_input():
        """Prompt for and set total hours streamed."""
        data = load_hours_streamed()
        current = data.get("total_hours", 0)
        print(f"\n  Current total hours: {current}")
        print("  Enter new total hours: ", end='', flush=True)
        try:
            hours_input = input().strip()
            hours = float(hours_input)
            if hours >= 0:
                data["total_hours"] = hours
                save_hours_streamed(data)
                print(f"  + Hours updated: {current} -> {hours}")
            else:
                print("  - Hours cannot be negative")
        except ValueError:
            print("  - Please enter a number")

    def keyboard_listener():
        """Listen for keyboard input to manually add bits or set hours."""
        import sys

        # Platform-specific keyboard handling
        if sys.platform == 'win32':
            import msvcrt
            print("\nPress 'b' to manually add bits | 'h' to set total hours streamed")

            while True:
                try:
                    if msvcrt.kbhit():
                        char = msvcrt.getch().decode('utf-8', errors='ignore').lower()
                        if char == 'b':
                            print("\nEnter bits amount: ", end='', flush=True)
                            try:
                                bits_input = input().strip()
                                bits = int(bits_input)
                                if bits > 0:
                                    record_cheer(bits)
                                    print(f"  + Manually added: {bits} bits")
                                else:
                                    print("  - Invalid amount")
                            except ValueError:
                                print("  - Please enter a number")
                        elif char == 'h':
                            handle_hours_input()
                    time.sleep(0.1)
                except Exception:
                    time.sleep(1)
        else:
            # Unix/Linux/Mac - use select
            import select
            print("\nPress 'b' to manually add bits | 'h' to set total hours streamed")

            while True:
                try:
                    if sys.stdin in select.select([sys.stdin], [], [], 0)[0]:
                        line = sys.stdin.readline().strip()
                        if line.lower() == 'b':
                            print("\nEnter bits amount: ", end='', flush=True)
                            try:
                                bits_input = input().strip()
                                bits = int(bits_input)
                                if bits > 0:
                                    record_cheer(bits)
                                    print(f"  + Manually added: {bits} bits")
                                else:
                                    print("  - Invalid amount")
                            except ValueError:
                                print("  - Please enter a number")
                        elif line.lower() == 'h':
                            handle_hours_input()
                    time.sleep(0.1)
                except Exception:
                    time.sleep(1)

    # Start keyboard listener in background thread
    keyboard_thread = threading.Thread(target=keyboard_listener, daemon=True)
    keyboard_thread.start()
    
    try:
        while True:
            stats = fetch_all_stats(config)
            if stats:
                save_to_archive(stats)
                save_latest(stats)
                timestamp = stats['timestamp'][:19]
                gifted_revenue = (stats.get('gifted_tiers', {}).get('1000', 0) * 4.19 +
                                 stats.get('gifted_tiers', {}).get('2000', 0) * 6.99 +
                                 stats.get('gifted_tiers', {}).get('3000', 0) * 17.49)
                print(f"[{timestamp}] Subs: {stats['subscribers']} | Followers: {stats['followers']} | "
                      f"Bits: ${stats['bits_today']:.2f} today | "
                      f"Hours: {stats.get('hours_session', 0):.1f}h session, {stats.get('hours_total', 0):.1f}h total | "
                      f"Artist: ${stats['artist_raise_30']:.2f} mo | ${stats.get('artist_yearly', 0.0):.2f} life")
            else:
                print(f"[{datetime.now().isoformat()[:19]}] Failed to fetch stats")
            time.sleep(config["poll_interval_seconds"])
    except KeyboardInterrupt:
        print("\nTracker stopped.")
