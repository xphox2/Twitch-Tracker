#!/usr/bin/env python3
"""
Twitch Stats Tracker
Downloads and archives your Twitch stats locally for OBS integration.
Tracks: subscribers, bits, earnings (70%), artist donations (30%)
Includes built-in HTTP server for OBS Browser Source overlay.
"""

import requests
import json
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

CONFIG_FILE = "config.json"
DATA_DIR = "data"
ARCHIVE_DIR = os.path.join(DATA_DIR, "archive", "daily")
LATEST_FILE = os.path.join(DATA_DIR, "latest_stats.json")
ARTIST_EARNINGS_FILE = os.path.join(DATA_DIR, "artist_earnings.json")
DAILY_BITS_FILE = os.path.join(DATA_DIR, "daily_bits.json")
BITS_TRACK_FILE = os.path.join(DATA_DIR, "bits_tracker.json")
SUB_TIERS_FILE = os.path.join(DATA_DIR, "sub_tiers.json")
SUBSCRIBERS_FILE = os.path.join(DATA_DIR, "subscribers.json")
ARCHIVE_RETENTION_DAYS = 365

BITS_VALUE_USD_PER_BIT = 0.01

# Background threads update bits + artist earnings; protect file updates.
BITS_TRACK_LOCK = threading.Lock()
ARTIST_EARNINGS_LOCK = threading.Lock()

TIER_VALUES = {
    "1000": 2.50,
    "2000": 5.00,
    "3000": 24.99
}

# Notes:
# - Followers endpoint uses moderator:read:followers
# - Bits are tracked via IRC (requires chat:read)
SCOPES = ["channel:read:subscriptions", "moderator:read:followers", "chat:read"]

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
    if os.path.exists(SUBSCRIBERS_FILE):
        with open(SUBSCRIBERS_FILE, 'r') as f:
            return json.load(f)
    return {"subs": {}, "history": []}

def save_subscribers(data):
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp_path = SUBSCRIBERS_FILE + ".tmp"
    with open(tmp_path, 'w') as f:
        json.dump(data, f, indent=2)
    os.replace(tmp_path, SUBSCRIBERS_FILE)

def sync_subscribers(current_subs):
    data = load_subscribers()

    # Use user_id as the stable key (Helix returns user_id; not an object id).
    current_ids = {str(sub.get("user_id")) for sub in current_subs if sub.get("user_id")}
    subs_by_id = data.get("subs", {})
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

    if len(new_ids) > 0:
        print(f"  + {len(new_ids)} new subscriber(s) detected")
        for user_id in sorted(new_ids):
            sub = current_subs_by_id.get(user_id, {})
            tier = sub.get("tier", "unknown")
            print(f"    - {sub.get('user_name', 'Unknown')}: Tier {tier}, Gift: {sub.get('is_gift', False)}")

    if len(ended_ids) > 0:
        print(f"  - {len(ended_ids)} subscriber(s) ended")

    return {
        "total": len(current_subs),
        "new": len(new_ids),
        "ended": len(ended_ids)
    }

def load_sub_tiers():
    if os.path.exists(SUB_TIERS_FILE):
        with open(SUB_TIERS_FILE, 'r') as f:
            return json.load(f)
    return {
        "total_subs_revenue": 0,
        "gifted_subs_revenue": 0,
        "nongift_subs_revenue": 0,
        "tiers": {"1000": 0, "2000": 0, "3000": 0},
        "gifted_tiers": {"1000": 0, "2000": 0, "3000": 0}
    }

def save_sub_tiers(data):
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp_path = SUB_TIERS_FILE + ".tmp"
    with open(tmp_path, 'w') as f:
        json.dump(data, f, indent=2)
    os.replace(tmp_path, SUB_TIERS_FILE)

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

def _parse_irc_tags(tag_str):
    tags = {}
    for part in tag_str.split(';'):
        if '=' in part:
            k, v = part.split('=', 1)
            tags[k] = v
        else:
            tags[part] = ""
    return tags

def start_bits_irc_listener(config):
    """Background listener that tracks cheers (bits) via Twitch IRC."""
    token = (config or {}).get("access_token")
    username = (config or {}).get("username")
    channel = (config or {}).get("channel") or username
    if not token or not username or not channel:
        print("WARNING: IRC bits tracker disabled (missing username/channel/token in config).")
        return

    def run():
        server = "irc.chat.twitch.tv"
        port = 6697
        while True:
            s = None
            try:
                raw = socket.create_connection((server, port), timeout=15)
                ctx = ssl.create_default_context()
                s = ctx.wrap_socket(raw, server_hostname=server)
                s.settimeout(60)

                sock = s

                def send(line):
                    sock.sendall((line + "\r\n").encode("utf-8"))

                send("CAP REQ :twitch.tv/tags twitch.tv/commands twitch.tv/membership")
                send(f"PASS oauth:{token}")
                send(f"NICK {username}")
                send(f"JOIN #{channel.lower()}")

                print(f"IRC bits tracker connected: #{channel.lower()}")

                buf = b""
                while True:
                    chunk = s.recv(4096)
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

                        if msg.startswith("@"):
                            tag_part, rest = msg.split(" ", 1)
                            tags = _parse_irc_tags(tag_part[1:])
                            bits = tags.get("bits")
                            if bits:
                                record_cheer(bits)
            except Exception as e:
                print(f"WARNING: IRC bits tracker error: {e}. Reconnecting...")
                time.sleep(5)
            finally:
                try:
                    if s:
                        s.close()
                except Exception:
                    pass

    t = threading.Thread(target=run, daemon=True)
    t.start()

def calculate_earnings(bits, donations):
    total_revenue = bits + donations
    creator_earnings = total_revenue * 0.70
    artist_raise = total_revenue * 0.30
    return creator_earnings, artist_raise

def load_artist_earnings():
    if os.path.exists(ARTIST_EARNINGS_FILE):
        with open(ARTIST_EARNINGS_FILE, 'r') as f:
            return json.load(f)
    return {"monthly_bits": {}, "monthly_subs": {}, "monthly_gifted": {}, "lifetime_bits": 0, "lifetime_subs": 0, "lifetime_gifted": 0, "lifetime_total": 0}

def save_artist_earnings(data):
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp_path = ARTIST_EARNINGS_FILE + ".tmp"
    with open(tmp_path, 'w') as f:
        json.dump(data, f, indent=2)
    os.replace(tmp_path, ARTIST_EARNINGS_FILE)

def load_daily_bits():
    if os.path.exists(DAILY_BITS_FILE):
        with open(DAILY_BITS_FILE, 'r') as f:
            return json.load(f)
    return {"date": None, "bits": 0, "total_bits": 0}

def save_daily_bits(data):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(DAILY_BITS_FILE, 'w') as f:
        json.dump(data, f, indent=2)

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
    sub_data = load_sub_tiers()
    total_subs_revenue = sum(count * TIER_VALUES.get(tier, 0) for tier, count in current_tiers.items())
    gifted_subs_revenue = sum(count * TIER_VALUES.get(tier, 0) for tier, count in gifted_tiers.items())
    nongift_subs_revenue = max(0.0, total_subs_revenue - gifted_subs_revenue)

    # Backwards-compatible migration: if the new revenue buckets aren't present,
    # seed them from current values and don't treat this as "new" revenue.
    if "gifted_subs_revenue" not in sub_data or "nongift_subs_revenue" not in sub_data:
        sub_data["gifted_subs_revenue"] = round(float(gifted_subs_revenue), 2)
        sub_data["nongift_subs_revenue"] = round(float(nongift_subs_revenue), 2)
        sub_data["total_subs_revenue"] = round(float(total_subs_revenue), 2)
        sub_data["tiers"] = current_tiers
        sub_data["gifted_tiers"] = gifted_tiers
        save_sub_tiers(sub_data)
        return 0.0, 0.0

    prev_gifted = float(sub_data.get("gifted_subs_revenue", 0) or 0)
    prev_nongift = float(sub_data.get("nongift_subs_revenue", 0) or 0)

    daily_gifted_increment = max(0.0, gifted_subs_revenue - prev_gifted)
    daily_nongift_increment = max(0.0, nongift_subs_revenue - prev_nongift)

    sub_data["total_subs_revenue"] = round(float(total_subs_revenue), 2)
    sub_data["gifted_subs_revenue"] = round(float(gifted_subs_revenue), 2)
    sub_data["nongift_subs_revenue"] = round(float(nongift_subs_revenue), 2)
    sub_data["tiers"] = current_tiers
    sub_data["gifted_tiers"] = gifted_tiers
    save_sub_tiers(sub_data)

    return daily_nongift_increment, daily_gifted_increment

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

    try:
        current_tiers, current_subs = get_sub_tiers(client_id, token, broadcaster_id)
        gifted_tiers = count_gifted_subs(current_subs)
    except Exception as e:
        print(f"ERROR fetching sub tiers: {e}")
        current_tiers = {"1000": 0, "2000": 0, "3000": 0}
        current_subs = []
        gifted_tiers = {"1000": 0, "2000": 0, "3000": 0}

    sync_subscribers(current_subs)

    sub_data = load_sub_tiers()

    if subscribers == 0 and sub_data.get("tiers"):
        total_manual_subs = sum(sub_data["tiers"].values())
        if total_manual_subs > 0:
            subscribers = total_manual_subs

    if not current_tiers["1000"] and not current_tiers["2000"] and not current_tiers["3000"]:
        if sub_data.get("tiers"):
            current_tiers = sub_data["tiers"]

    bits_data = load_bits_tracker()
    bits_today_revenue = float(bits_data.get("today_bits_revenue", 0.0) or 0.0)
    bits_total_revenue = float(bits_data.get("lifetime_bits_revenue", 0.0) or 0.0)

    total_subs_revenue = sum(count * TIER_VALUES[tier] for tier, count in current_tiers.items())

    if total_subs_revenue == 0 and sub_data.get("total_subs_revenue", 0) > 0:
        total_subs_revenue = sub_data["total_subs_revenue"]

    daily_nongift_increment, daily_gifted_increment = update_totals(current_tiers, gifted_tiers)

    daily_artist_subs = daily_nongift_increment * 0.30
    daily_artist_gifted = daily_gifted_increment * 0.30

    # Bits are tracked via IRC in record_cheer() (real-time). Only apply subs/gifted deltas here.
    monthly_artist_bits, monthly_artist_subs, monthly_artist_gifted, lifetime_total = update_artist_earnings(0.0, daily_artist_subs, daily_artist_gifted)

    return {
        "timestamp": datetime.now().isoformat(),
        "subscribers": subscribers,
        "followers": followers,
        "bits_today": round(bits_today_revenue, 2),
        "bits_total": round(bits_total_revenue, 2),
        "subs_today_revenue": round(daily_nongift_increment + daily_gifted_increment, 2),
        "subs_total_revenue": round(total_subs_revenue, 2),
        "artist_bits_monthly": round(monthly_artist_bits, 2),
        "artist_subs_monthly": round(monthly_artist_subs, 2),
        "artist_gifted_monthly": round(monthly_artist_gifted, 2),
        "artist_raise_30": round(monthly_artist_bits + monthly_artist_subs + monthly_artist_gifted, 2),
        "artist_yearly": round(lifetime_total, 2),
        "total_revenue": round(bits_total_revenue + total_subs_revenue, 2),
        "tiers": current_tiers,
        "gifted_tiers": gifted_tiers
    }

def save_to_archive(stats):
    rotate_archive_if_needed()
    archive_file = get_archive_filename()
    file_exists = os.path.exists(archive_file)

    with open(archive_file, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=stats.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(stats)

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
        tier1 = int(input("  Tier 1 ($2.50) subs: ").strip())
        tier2 = int(input("  Tier 2 ($5.00) subs: ").strip())
        tier3 = int(input("  Tier 3 ($24.99) subs: ").strip())
    except ValueError:
        tier1 = tier2 = tier3 = 0

    total_subs_revenue = (tier1 * 2.50) + (tier2 * 5.00) + (tier3 * 24.99)

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

    artist_subs_lifetime = total_subs_revenue * 0.30

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
    print(f"  Tier 1: {tier1} x $2.50")
    print(f"  Tier 2: {tier2} x $5.00")
    print(f"  Tier 3: {tier3} x $24.99")
    print(f"  Subs Revenue: ${total_subs_revenue:.2f}")
    print(f"  Artist Subs Lifetime: ${artist_subs_lifetime:.2f}")
    print(f"\nRun 'python twitch_stats.py' to start tracking")

def import_csv_history(folder):
    print(f"\nImporting CSV history from: {folder}")
    print("=" * 40)

    if not os.path.exists(folder):
        print(f"Folder not found: {folder}")
        return

    csv_files = glob.glob(os.path.join(folder, "*.csv"))
    if not csv_files:
        print("No CSV files found in folder")
        return

    earnings_data = load_artist_earnings()
    sub_data = load_sub_tiers()
    imported_bits_revenue_total = 0.0

    for csv_file in sorted(csv_files):
        filename = os.path.basename(csv_file)
        print(f"\nProcessing: {filename}")

        try:
            with open(csv_file, 'r') as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    print(f"  Empty file")
                    continue

                print(f"  Columns: {', '.join(reader.fieldnames)}")

                for row in reader:
                    date = row.get('Date', '').strip()
                    if not date:
                        continue

                    bits = 0
                    bits_col = row.get('Bits Revenue', '').strip()
                    if bits_col:
                        try:
                            bits = float(bits_col.replace('$', '').replace(',', ''))
                        except:
                            pass

                    if bits > 0:
                        imported_bits_revenue_total += bits

                    tier1 = tier2 = tier3 = 0
                    tier1_col = row.get('Tier 1 subs', '').strip()
                    tier2_col = row.get('Tier 2 subs', '').strip()
                    tier3_col = row.get('Tier 3 subs', '').strip()

                    if tier1_col:
                        try:
                            tier1 = int(tier1_col.replace(',', ''))
                        except:
                            pass
                    if tier2_col:
                        try:
                            tier2 = int(tier2_col.replace(',', ''))
                        except:
                            pass
                    if tier3_col:
                        try:
                            tier3 = int(tier3_col.replace(',', ''))
                        except:
                            pass

                    if bits == 0 and tier1 == 0 and tier2 == 0 and tier3 == 0:
                        continue

                    try:
                        month = datetime.strptime(date, "%Y-%m-%d").strftime("%Y-%m")
                    except ValueError:
                        try:
                            month = datetime.strptime(date, "%m/%d/%Y").strftime("%Y-%m")
                        except ValueError:
                            try:
                                month = datetime.strptime(date, "%a %b %d %Y").strftime("%Y-%m")
                            except ValueError:
                                try:
                                    month = datetime.strptime(date, "%Y-%m").strftime("%Y-%m")
                                except ValueError:
                                    print(f"  Skipping invalid date: {date}")
                                    continue

                    subs_revenue = (tier1 * 2.50) + (tier2 * 5.00) + (tier3 * 24.99)

                    artist_bits = bits * 0.30
                    artist_subs = subs_revenue * 0.30

                    current_monthly_bits = earnings_data["monthly_bits"].get(month, 0)
                    current_monthly_subs = earnings_data["monthly_subs"].get(month, 0)

                    earnings_data["monthly_bits"][month] = round(current_monthly_bits + artist_bits, 2)
                    earnings_data["monthly_subs"][month] = round(current_monthly_subs + artist_subs, 2)

                    print(f"  {month}: ${artist_bits:,.2f} artist bits (${bits:,.2f} total), Tier1={tier1}, Tier2={tier2}, Tier3={tier3}, Artist Subs=${artist_subs:,.2f}")

        except Exception as e:
            print(f"  Error reading {filename}: {e}")

    lifetime_bits = sum(earnings_data["monthly_bits"].values())
    lifetime_subs = sum(earnings_data["monthly_subs"].values())

    earnings_data["lifetime_bits"] = round(lifetime_bits, 2)
    earnings_data["lifetime_subs"] = round(lifetime_subs, 2)
    earnings_data["lifetime_total"] = round(lifetime_bits + lifetime_subs, 2)

    save_artist_earnings(earnings_data)

    # Seed bits lifetime totals from imported CSV revenue.
    try:
        bits_data = load_bits_tracker()
        bits_data["lifetime_bits_revenue_imported"] = round(float(imported_bits_revenue_total), 2)
        bits_data["lifetime_bits_revenue"] = round(
            float(bits_data.get("lifetime_bits_revenue_tracked", 0.0) or 0.0)
            + float(bits_data.get("lifetime_bits_revenue_imported", 0.0) or 0.0),
            2,
        )
        save_bits_tracker(bits_data)
    except Exception as e:
        print(f"WARNING: Failed to seed bits totals from CSV import: {e}")

    print(f"\n" + "=" * 40)
    print("Import complete!")
    print(f"  Artist Bits (30%) Lifetime: ${lifetime_bits:,.2f}")
    print(f"  Artist Subs (30%) Lifetime: ${lifetime_subs:,.2f}")
    print(f"  Artist Lifetime (30%): ${earnings_data['lifetime_total']:,.2f}")
    print(f"  Bits Revenue Imported (100%): ${imported_bits_revenue_total:,.2f}")
    print(f"\nRun 'python twitch_stats.py' to start tracking")

class StatsHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=os.path.dirname(os.path.abspath(__file__)), **kwargs)

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
        
        if parsed_path.path == '/api/save-branding':
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length > 0:
                post_data = self.rfile.read(content_length)
                
                try:
                    branding_config = json.loads(post_data.decode('utf-8'))
                    
                    # Generate the branding-setup.js content
                    js_content = generate_branding_js(branding_config)
                    
                    # Write to file
                    with open('branding-setup.js', 'w') as f:
                        f.write(js_content)
                    
                    # Send success response
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({
                        'success': True,
                        'message': 'Branding saved successfully!'
                    }).encode())
                    
                    print(f"✓ Branding updated: {branding_config.get('fontFamily', 'Unknown')} font")
                    
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

def generate_branding_js(config):
    """Generate the modular branding-setup.js file content from config"""
    widgets = config.get('widgets', {})
    
    followers = widgets.get('followers', {})
    monthly = widgets.get('monthly', {})
    lifetime = widgets.get('lifetime', {})
    
    return f'''// Central Configuration for All Widgets
// Generated by Branding Settings
// Modular - Each widget has independent styling

const DEFAULT_CONFIG = {{
    fontFamily: "{config.get('fontFamily', 'Bebas Neue')}",
    googleFont: {str(config.get('googleFont', True)).lower()},
    fontWeight: "bold",
    textShadow: "2px 2px 4px rgba(0,0,0,0.8)",
    
    layout: {{
        gap: {config.get('layout', {}).get('gap', 5)}
    }},
    
    widgets: {{
        followers: {{
            fontSize: {followers.get('fontSize', 48)},
            labelFontSize: {followers.get('labelFontSize', 14)},
            colors: {{
                text: "{followers.get('colors', {}).get('text', '#9146ff')}",
                label: "{followers.get('colors', {}).get('label', '#aaaaaa')}",
                border: "{followers.get('colors', {}).get('border', '#9146ff')}",
                background: "{followers.get('colors', {}).get('background', 'rgba(0,0,0,0.3)')}"
            }},
            border: {{
                width: {followers.get('border', {}).get('width', 2)},
                style: "{followers.get('border', {}).get('style', 'solid')}",
                radius: {followers.get('border', {}).get('radius', 0)}
            }},
            background: {{
                enabled: {str(followers.get('background', {}).get('enabled', False)).lower()},
                color: "{followers.get('background', {}).get('color', 'rgba(0,0,0,0.3)')}",
                opacity: {followers.get('background', {}).get('opacity', 30)}
            }},
            padding: {{
                vertical: {followers.get('padding', {}).get('vertical', 8)},
                horizontal: {followers.get('padding', {}).get('horizontal', 15)}
            }}
        }},
        monthly: {{
            fontSize: {monthly.get('fontSize', 42)},
            labelFontSize: {monthly.get('labelFontSize', 14)},
            colors: {{
                text: "{monthly.get('colors', {}).get('text', '#ff6b6b')}",
                label: "{monthly.get('colors', {}).get('label', '#aaaaaa')}",
                border: "{monthly.get('colors', {}).get('border', '#ff6b6b')}",
                background: "{monthly.get('colors', {}).get('background', 'rgba(0,0,0,0.3)')}"
            }},
            border: {{
                width: {monthly.get('border', {}).get('width', 2)},
                style: "{monthly.get('border', {}).get('style', 'solid')}",
                radius: {monthly.get('border', {}).get('radius', 0)}
            }},
            background: {{
                enabled: {str(monthly.get('background', {}).get('enabled', False)).lower()},
                color: "{monthly.get('background', {}).get('color', 'rgba(0,0,0,0.3)')}",
                opacity: {monthly.get('background', {}).get('opacity', 30)}
            }},
            padding: {{
                vertical: {monthly.get('padding', {}).get('vertical', 8)},
                horizontal: {monthly.get('padding', {}).get('horizontal', 15)}
            }}
        }},
        lifetime: {{
            fontSize: {lifetime.get('fontSize', 36)},
            labelFontSize: {lifetime.get('labelFontSize', 14)},
            colors: {{
                text: "{lifetime.get('colors', {}).get('text', '#ffd700')}",
                label: "{lifetime.get('colors', {}).get('label', '#aaaaaa')}",
                border: "{lifetime.get('colors', {}).get('border', '#ffd700')}",
                background: "{lifetime.get('colors', {}).get('background', 'rgba(0,0,0,0.3)')}"
            }},
            border: {{
                width: {lifetime.get('border', {}).get('width', 2)},
                style: "{lifetime.get('border', {}).get('style', 'solid')}",
                radius: {lifetime.get('border', {}).get('radius', 0)}
            }},
            background: {{
                enabled: {str(lifetime.get('background', {}).get('enabled', False)).lower()},
                color: "{lifetime.get('background', {}).get('color', 'rgba(0,0,0,0.3)')}",
                opacity: {lifetime.get('background', {}).get('opacity', 30)}
            }},
            padding: {{
                vertical: {lifetime.get('padding', {}).get('vertical', 8)},
                horizontal: {lifetime.get('padding', {}).get('horizontal', 15)}
            }}
        }}
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
    
    const widgets = ['followers', 'monthly', 'lifetime'];
    
    widgets.forEach(widget => {{
        const config = WIDGET_CONFIG.widgets[widget];
        if (!config) return;
        
        const prefix = '--widget-' + widget;
        
        root.style.setProperty(prefix + '-font-size', config.fontSize + 'px');
        root.style.setProperty(prefix + '-label-font-size', config.labelFontSize + 'px');
        root.style.setProperty(prefix + '-text-color', config.colors.text);
        root.style.setProperty(prefix + '-label-color', config.colors.label);
        root.style.setProperty(prefix + '-border-color', config.colors.border);
        
        if (config.background.enabled) {{
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
        
        root.style.setProperty(prefix + '-border-width', config.border.width + 'px');
        root.style.setProperty(prefix + '-border-style', config.border.style);
        root.style.setProperty(prefix + '-border-radius', config.border.radius + 'px');
        root.style.setProperty(prefix + '-padding', config.padding.vertical + 'px ' + config.padding.horizontal + 'px');
    }});
}}

document.addEventListener('DOMContentLoaded', applyWidgetStyles);
'''

def start_server(port=3001):
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    with socketserver.TCPServer(("", port), StatsHandler) as httpd:
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
                print("\nCSV Format:")
                print("  date,bits,tier1,tier2,tier3")
                print("  2024-08,15000,100,25,5")
                print("\nPlace CSV files in the folder and run this command.")
                sys.exit(1)
            import_csv_history(sys.argv[2])
        elif sys.argv[1] == "--simulate-cheer":
            if len(sys.argv) < 3:
                print("Usage: python twitch_stats.py --simulate-cheer <bits>")
                sys.exit(1)
            bits = sys.argv[2]
            record_cheer(bits)
            bits_data = load_bits_tracker()
            print(f"Bits Totals: ${bits_data.get('today_bits_revenue', 0.0):.2f} today, ${bits_data.get('lifetime_bits_revenue', 0.0):.2f} lifetime")
        else:
            print("Usage: python twitch_stats.py [--setup] [--server <port>] [--set-totals] [--import-csvs <folder>]")
            print("\nExamples:")
            print("  python twitch_stats.py              # Start tracker with built-in web server")
            print("  python twitch_stats.py --server     # Web server only (no Twitch API)")
            print("  python twitch_stats.py --setup      # Configure Twitch API credentials")
            print("  python twitch_stats.py --simulate-cheer 100  # Local test: add $1.00 bits revenue")
        sys.exit(0)

    config = load_config()
    if not config:
        config = setup_config()
    if not config:
        print("Setup failed. Please run 'python twitch_stats.py --setup' again.")
        sys.exit(1)

    print("\n" + "=" * 50)
    print("Twitch Stats Tracker")
    print("=" * 50)
    print(f"Stats Overlay: http://localhost:3001/obs_overlay.html")
    print(f"Branding Editor: http://localhost:3001/branding.html")
    print(f"Sidebar2: http://localhost:3001/sidebar2.html")
    print(f"Data archive: {ARCHIVE_DIR}/stats_archive_YYYY-MM-DD.csv")
    print(f"Retention: Last {ARCHIVE_RETENTION_DAYS} days")
    print("Press Ctrl+C to stop\n")

    server_thread = threading.Thread(target=start_server, daemon=True)
    server_thread.start()

    if config.get("enable_irc_bits", True):
        start_bits_irc_listener(config)

    try:
        while True:
            stats = fetch_all_stats(config)
            if stats:
                save_to_archive(stats)
                save_latest(stats)
                timestamp = stats["timestamp"][:19]
                gifted_revenue = (stats.get('gifted_tiers', {}).get('1000', 0) * 2.50 +
                                 stats.get('gifted_tiers', {}).get('2000', 0) * 5.00 +
                                 stats.get('gifted_tiers', {}).get('3000', 0) * 24.99)
                print(f"[{timestamp}] Subs: {stats['subscribers']} | Followers: {stats['followers']} | "
                      f"Bits: ${stats['bits_today']:.2f} today, ${stats['bits_total']:.2f} total | "
                      f"Subs: {stats['subs_today_revenue']:.2f} today, {stats['subs_total_revenue']:.2f} total | "
                      f"Tiers: T1={stats['tiers'].get('1000', 0)} T2={stats['tiers'].get('2000', 0)} T3={stats['tiers'].get('3000', 0)} | "
                      f"Gifted: {stats.get('gifted_tiers', {}).get('1000', 0)}/{stats.get('gifted_tiers', {}).get('2000', 0)}/{stats.get('gifted_tiers', {}).get('3000', 0)} | "
                      f"Artist: ${stats['artist_raise_30']:.2f} mo")
            else:
                print(f"[{datetime.now().isoformat()[:19]}] Failed to fetch stats")
            time.sleep(config["poll_interval_seconds"])
    except KeyboardInterrupt:
        print("\nTracker stopped.")
