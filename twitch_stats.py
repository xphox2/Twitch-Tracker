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
SUB_TIERS_FILE = os.path.join(DATA_DIR, "sub_tiers.json")
SUBSCRIBERS_FILE = os.path.join(DATA_DIR, "subscribers.json")
ARCHIVE_RETENTION_DAYS = 365

TIER_VALUES = {
    "1000": 2.50,
    "2000": 5.00,
    "3000": 24.99
}

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

def get_subscribers_list(client_id, oauth, broadcaster_id):
    url = "https://api.twitch.tv/helix/subscriptions"
    headers = {
        "Client-ID": client_id,
        "Authorization": f"Bearer {oauth}"
    }
    params = {"broadcaster_id": broadcaster_id, "first": 100}
    subs = []
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json().get("data", [])
        for sub in data:
            subs.append({
                "id": sub.get("id"),
                "user_name": sub.get("user_name"),
                "tier": sub.get("tier"),
                "is_gift": sub.get("is_gift", False),
                "created_at": sub.get("created_at")
            })
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
    with open(SUBSCRIBERS_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def sync_subscribers(current_subs):
    data = load_subscribers()

    current_ids = {sub["id"] for sub in current_subs}
    stored_ids = set(data["subs"].keys())

    new_ids = current_ids - stored_ids
    ended_ids = stored_ids - current_ids

    timestamp = datetime.now().isoformat()

    for sub in current_subs:
        if sub["id"] in new_ids:
            sub["status"] = "active"
            sub["started_at"] = timestamp
            sub["gifts_received"] = 0
            data["subs"][sub["id"]] = sub
            data["history"].append({
                "type": "new",
                "user_id": sub["id"],
                "user_name": sub["user_name"],
                "tier": sub["tier"],
                "is_gift": sub["is_gift"],
                "timestamp": timestamp
            })

    for user_id in ended_ids:
        if user_id in data["subs"]:
            data["subs"][user_id]["status"] = "ended"
            data["subs"][user_id]["ended_at"] = timestamp
            data["history"].append({
                "type": "ended",
                "user_id": user_id,
                "user_name": data["subs"][user_id].get("user_name"),
                "timestamp": timestamp
            })

    save_subscribers(data)

    return {
        "total": len(current_subs),
        "new": len(new_ids),
        "ended": len(ended_ids)
    }

def load_sub_tiers():
    if os.path.exists(SUB_TIERS_FILE):
        with open(SUB_TIERS_FILE, 'r') as f:
            return json.load(f)
    return {"total_bits": 0, "total_subs_revenue": 0, "tiers": {"1000": 0, "2000": 0, "3000": 0}}

def save_sub_tiers(data):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(SUB_TIERS_FILE, 'w') as f:
        json.dump(data, f, indent=2)

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
    return {"monthly_bits": {}, "monthly_subs": {}, "lifetime_bits": 0, "lifetime_subs": 0, "lifetime_total": 0}

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

def update_artist_earnings(daily_artist_bits, daily_artist_subs):
    data = load_artist_earnings()
    current_month = get_current_month_key()

    last_monthly_bits = data["monthly_bits"].get(current_month, 0)
    last_monthly_subs = data["monthly_subs"].get(current_month, 0)

    new_monthly_bits = last_monthly_bits + daily_artist_bits
    new_monthly_subs = last_monthly_subs + daily_artist_subs

    data["monthly_bits"][current_month] = round(new_monthly_bits, 2)
    data["monthly_subs"][current_month] = round(new_monthly_subs, 2)

    lifetime_bits = data["lifetime_bits"] + daily_artist_bits
    lifetime_subs = data["lifetime_subs"] + daily_artist_subs
    lifetime_total = lifetime_bits + lifetime_subs

    data["lifetime_bits"] = round(lifetime_bits, 2)
    data["lifetime_subs"] = round(lifetime_subs, 2)
    data["lifetime_total"] = round(lifetime_total, 2)

    save_artist_earnings(data)
    return new_monthly_bits, new_monthly_subs, data["lifetime_total"]

def update_totals(current_bits, current_tiers, lifetime_bits, lifetime_subs):
    sub_data = load_sub_tiers()
    daily_bits_increment = max(0, current_bits - sub_data["total_bits"])
    sub_data["total_bits"] = current_bits

    total_subs_revenue = sum(count * TIER_VALUES[tier] for tier, count in current_tiers.items())
    daily_subs_increment = max(0, total_subs_revenue - sub_data.get("total_subs_revenue", 0))
    sub_data["total_subs_revenue"] = total_subs_revenue
    sub_data["tiers"] = current_tiers
    save_sub_tiers(sub_data)

    return daily_bits_increment, daily_subs_increment

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
    current_tiers, current_subs = get_sub_tiers(client_id, token, broadcaster_id)

    sync_subscribers(current_subs)

    sub_data = load_sub_tiers()

    if subscribers == 0 and sub_data.get("tiers"):
        total_manual_subs = sum(sub_data["tiers"].values())
        if total_manual_subs > 0:
            subscribers = total_manual_subs

    if not current_tiers["1000"] and not current_tiers["2000"] and not current_tiers["3000"]:
        if sub_data.get("tiers"):
            current_tiers = sub_data["tiers"]

    daily_bits, daily_increment = update_daily_bits(current_bits)

    total_subs_revenue = sum(count * TIER_VALUES[tier] for tier, count in current_tiers.items())

    if total_subs_revenue == 0 and sub_data.get("total_subs_revenue", 0) > 0:
        total_subs_revenue = sub_data["total_subs_revenue"]

    daily_bits_increment, daily_subs_increment = update_totals(current_bits, current_tiers, 0, 0)

    daily_artist_bits = daily_bits_increment * 0.30
    daily_artist_subs = daily_subs_increment * 0.30

    monthly_artist_bits, monthly_artist_subs, lifetime_total = update_artist_earnings(daily_artist_bits, daily_artist_subs)

    return {
        "timestamp": datetime.now().isoformat(),
        "subscribers": subscribers,
        "followers": followers,
        "bits_today": daily_bits,
        "bits_total": current_bits,
        "subs_today_revenue": round(daily_subs_increment, 2),
        "subs_total_revenue": round(total_subs_revenue, 2),
        "artist_bits_monthly": round(monthly_artist_bits, 2),
        "artist_subs_monthly": round(monthly_artist_subs, 2),
        "artist_raise_30": round(monthly_artist_bits + monthly_artist_subs, 2),
        "artist_yearly": round(lifetime_total, 2),
        "total_revenue": round(current_bits + total_subs_revenue, 2),
        "tiers": current_tiers
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

    sub_data = {
        "total_bits": 0,
        "total_subs_revenue": total_subs_revenue,
        "tiers": {
            "1000": tier1,
            "2000": tier2,
            "3000": tier3
        }
    }
    save_sub_tiers(sub_data)

    artist_subs_lifetime = total_subs_revenue * 0.30

    earnings_data = {
        "monthly_bits": {},
        "monthly_subs": {},
        "lifetime_bits": 0,
        "lifetime_subs": round(artist_subs_lifetime, 2),
        "lifetime_total": round(artist_subs_lifetime, 2)
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

    for csv_file in sorted(csv_files):
        filename = os.path.basename(csv_file)
        print(f"\nProcessing: {filename}")

        try:
            with open(csv_file, 'r') as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    print(f"  Empty file")
                    continue

                fieldnames = [col.lower().strip() for col in reader.fieldnames]
                print(f"  Columns: {', '.join(reader.fieldnames)}")

                for row in reader:
                    row_lower = {k.lower().strip(): v for k, v in row.items()}

                    date = (row_lower.get('date') or row_lower.get('month') or
                            row_lower.get('time') or row_lower.get('period') or '').strip()
                    if not date:
                        continue

                    bits = 0
                    for col in ['bits used', 'bits', 'total bits', 'bits_used']:
                        if col in row_lower and row_lower[col]:
                            try:
                                bits = float(row_lower[col].replace(',', ''))
                                break
                            except:
                                pass

                    tier1 = tier2 = tier3 = 0
                    for col, target in [('tier 1', 1), ('tier1', 1), ('tier_1', 1),
                                       ('tier 2', 2), ('tier2', 2), ('tier_2', 2),
                                       ('tier 3', 3), ('tier3', 3), ('tier_3', 3)]:
                        if col in row_lower and row_lower[col]:
                            try:
                                val = int(row_lower[col].replace(',', ''))
                                if target == 1:
                                    tier1 = val
                                elif target == 2:
                                    tier2 = val
                                else:
                                    tier3 = val
                            except:
                                pass

                    if tier1 == 0:
                        for col in ['tier1', 'tier 1 subscriptions']:
                            if col in row_lower and row_lower[col]:
                                try:
                                    tier1 = int(row_lower[col].replace(',', ''))
                                    break
                                except:
                                    pass
                    if tier2 == 0:
                        for col in ['tier2', 'tier 2 subscriptions']:
                            if col in row_lower and row_lower[col]:
                                try:
                                    tier2 = int(row_lower[col].replace(',', ''))
                                    break
                                except:
                                    pass
                    if tier3 == 0:
                        for col in ['tier3', 'tier 3 subscriptions']:
                            if col in row_lower and row_lower[col]:
                                try:
                                    tier3 = int(row_lower[col].replace(',', ''))
                                    break
                                except:
                                    pass

                    if tier1 == 0 and tier2 == 0 and tier3 == 0:
                        continue

                    try:
                        month = datetime.strptime(date, "%Y-%m").strftime("%Y-%m")
                    except ValueError:
                        try:
                            month = datetime.strptime(date, "%Y-%m-%d").strftime("%Y-%m")
                        except ValueError:
                            print(f"  Skipping invalid date: {date}")
                            continue

                    subs_revenue = (tier1 * 2.50) + (tier2 * 5.00) + (tier3 * 24.99)

                    current_monthly_bits = earnings_data["monthly_bits"].get(month, 0)
                    current_monthly_subs = earnings_data["monthly_subs"].get(month, 0)

                    if bits > current_monthly_bits:
                        earnings_data["monthly_bits"][month] = bits
                    if subs_revenue > current_monthly_subs:
                        earnings_data["monthly_subs"][month] = subs_revenue

                    print(f"  {month}: {bits:,.0f} bits, Tier1={tier1}, Tier2={tier2}, Tier3={tier3}")

        except Exception as e:
            print(f"  Error reading {filename}: {e}")

    lifetime_bits = sum(earnings_data["monthly_bits"].values())
    lifetime_subs = sum(earnings_data["monthly_subs"].values())

    earnings_data["lifetime_bits"] = round(lifetime_bits, 2)
    earnings_data["lifetime_subs"] = round(lifetime_subs, 2)
    earnings_data["lifetime_total"] = round(lifetime_bits + lifetime_subs, 2)

    save_artist_earnings(earnings_data)

    print(f"\n" + "=" * 40)
    print("Import complete!")
    print(f"  Lifetime Bits: {lifetime_bits:,.0f}")
    print(f"  Lifetime Subs Revenue: ${lifetime_subs:,.2f}")
    print(f"  Artist Lifetime: ${earnings_data['lifetime_total']:,.2f}")
    print(f"\nRun 'python twitch_stats.py' to start tracking")

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
        else:
            print("Usage: python twitch_stats.py [--setup] [--server <port>] [--set-totals] [--import-csvs <folder>]")
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
                      f"Bits: {stats['bits_today']} today, {stats['bits_total']} total | "
                      f"Subs Revenue: ${stats['subs_today_revenue']:.2f} today, ${stats['subs_total_revenue']:.2f} total | "
                      f"Artist: ${stats['artist_raise_30']:.2f} mo, ${stats['artist_yearly']:.2f} life")
            else:
                print(f"[{datetime.now().isoformat()[:19]}] Failed to fetch stats")
            time.sleep(config["poll_interval_seconds"])
    except KeyboardInterrupt:
        print("\nTracker stopped.")
