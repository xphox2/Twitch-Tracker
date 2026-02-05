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
from urllib.parse import urlencode
from datetime import datetime, timedelta
from pathlib import Path

CONFIG_FILE = "config.json"
DATA_DIR = "data"
ARCHIVE_DIR = os.path.join(DATA_DIR, "archive", "daily")
LATEST_FILE = os.path.join(DATA_DIR, "latest_stats.json")
ARTIST_EARNINGS_FILE = os.path.join(DATA_DIR, "artist_earnings.json")
DAILY_BITS_FILE = os.path.join(DATA_DIR, "daily_bits.json")
ARCHIVE_RETENTION_DAYS = 7

SCOPES = ["bits:read", "channel:read:subscriptions", "moderation:read"]

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
    params = {"broadcaster_id": broadcaster_id}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return len(response.json().get("data", []))
    return 0

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
    return 0

def get_bits(client_id, oauth, broadcaster_id):
    url = "https://api.twitch.tv/helix/bits/leaderboard"
    headers = {
        "Client-ID": client_id,
        "Authorization": f"Bearer {oauth}"
    }
    params = {"user_id": broadcaster_id, "count": 1}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json().get("data", [])
        if data:
            return data[0].get("score", 0)
    return 0

def calculate_earnings(bits, donations):
    total_revenue = bits + donations
    creator_earnings = total_revenue * 0.70
    artist_raise = total_revenue * 0.30
    return creator_earnings, artist_raise

def load_artist_earnings():
    if os.path.exists(ARTIST_EARNINGS_FILE):
        with open(ARTIST_EARNINGS_FILE, 'r') as f:
            return json.load(f)
    return {"monthly": {}, "lifetime_total": 0, "lifetime_donations": 0}

def save_artist_earnings(data):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(ARTIST_EARNINGS_FILE, 'w') as f:
        json.dump(data, f, indent=2)

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

def update_artist_earnings(daily_artist_bits):
    data = load_artist_earnings()
    current_month = get_current_month_key()

    last_monthly = data["monthly"].get(current_month, 0)
    new_monthly = last_monthly + daily_artist_bits

    data["monthly"][current_month] = new_monthly

    lifetime_total = 0
    for month_key, amount in data["monthly"].items():
        lifetime_total += amount

    data["lifetime_total"] = lifetime_total

    save_artist_earnings(data)
    return new_monthly, lifetime_total

def fetch_all_stats(config):
    client_id = config["client_id"]
    broadcaster_id = config["broadcaster_id"]

    token = config.get("access_token")
    if not token:
        print("No access token. Run setup again: python twitch_stats.py --setup")
        return None

    subscribers = get_subscribers(client_id, token, broadcaster_id)
    followers = get_followers(client_id, token, broadcaster_id)
    current_bits = get_bits(client_id, token, broadcaster_id)

    daily_bits, daily_increment = update_daily_bits(current_bits)

    daily_artist = daily_increment * 0.30
    lifetime_artist = current_bits * 0.30

    monthly_artist, lifetime_total = update_artist_earnings(daily_artist)

    return {
        "timestamp": datetime.now().isoformat(),
        "subscribers": subscribers,
        "followers": followers,
        "bits_today": daily_bits,
        "bits_total": current_bits,
        "artist_raise_30": round(monthly_artist, 2),
        "artist_yearly": round(lifetime_total, 2),
        "total_revenue": round(current_bits, 2)
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
    with open(LATEST_FILE, 'w') as f:
        json.dump(stats, f, indent=2)

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
        "poll_interval_seconds": 60
    }
    save_config(config)
    print(f"\nConfig saved! Run 'python twitch_stats.py' to start tracking")
    return config

class StatsHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=os.path.dirname(os.path.abspath(__file__)), **kwargs)

    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Cache-Control', 'no-cache')
        super().end_headers()

    def log_message(self, format, *args):
        pass

def start_server(port=3001):
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    with socketserver.TCPServer(("", port), StatsHandler) as httpd:
        print(f"Server: http://localhost:{port}/obs_overlay.html")
        httpd.serve_forever()

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        if sys.argv[1] == "--setup":
            setup_config()
        elif sys.argv[1] == "--server":
            port = int(sys.argv[2]) if len(sys.argv) > 2 else 3001
            print(f"Server: http://localhost:{port}/obs_overlay.html")
            start_server(port)
        else:
            print("Usage: python twitch_stats.py [--setup] [--server <port>]")
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
    print(f"Server: http://localhost:3001/obs_overlay.html")
    print(f"Data archive: {ARCHIVE_DIR}/stats_archive_YYYY-MM-DD.csv")
    print(f"Retention: Last {ARCHIVE_RETENTION_DAYS} days")
    print("Press Ctrl+C to stop\n")

    server_thread = threading.Thread(target=start_server, daemon=True)
    server_thread.start()

    try:
        while True:
            stats = fetch_all_stats(config)
            if stats:
                save_to_archive(stats)
                save_latest(stats)
                timestamp = stats["timestamp"][:19]
                print(f"[{timestamp}] Subs: {stats['subscribers']} | Followers: {stats['followers']} | "
                      f"Bits Today: {stats['bits_today']} | Bits Total: {stats['bits_total']} | "
                      f"Artist - Monthly: ${stats['artist_raise_30']:.2f} | Artist - Lifetime: ${stats['artist_yearly']:.2f}")
            else:
                print(f"[{datetime.now().isoformat()[:19]}] Failed to fetch stats")
            time.sleep(config["poll_interval_seconds"])
    except KeyboardInterrupt:
        print("\nTracker stopped.")
