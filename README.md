# Twitch Stats Tracker

Local Twitch stats tracking for OBS integration.

## Features
- Tracks subscribers, bits, and donations
- Calculates 70/30 split (creator earnings vs artist fund)
- Archives all data to CSV for historical records
- Real-time JSON for OBS integration
- Browser source overlay for OBS

## Setup

### 1. Get Twitch API Credentials
1. Go to https://dev.twitch.tv/console
2. Register your application
3. Copy your Client ID and Client Secret

### 2. Configure the Tracker
```bash
python twitch_stats.py --setup
```
Enter your credentials when prompted.

## Files

| File | Purpose |
|------|---------|
| `data/latest_stats.json` | Current stats (refresh every 5s) |
| `data/stats_archive.csv` | Full history with timestamps |
| `obs_overlay.html` | Browser source for OBS |

## OBS Integration

### Option 1: Browser Source
1. Add Browser Source in OBS
2. Local file: `obs_overlay.html`
3. Width: 800, Height: 200
4. Refresh: 5 seconds

http://localhost:3001/obs_overlay.html
http://localhost:3001/artists_combined.html
http://localhost:3001/artist_lifetime.html
http://localhost:3001/artist_monthly.html
http://localhost:3001/subscribers.html
http://localhost:3001/followers.html
http://localhost:3001/bits.html
http://localhost:3001/bits_total.html
http://localhost:3001/rotating_overlay.html
http://localhost:3001/rotating_overlay_2.html
http://localhost:3001/rotating_overlay_3.html

### Option 2: Read Latest Stats Directly
Read `data/latest_stats.json` in your preferred format.

## Automation

### Windows Task Scheduler
```bash
pythonw twitch_stats.py
```
Run on system startup for 24/7 tracking.

### Polling Interval
Default: 60 seconds (adjust in config.json)

## Data Format (latest_stats.json)
```json
{
  "timestamp": "2024-01-01T12:00:00",
  "subscribers": 100,
  "bits": 5000,
  "donations": 100.00,
  "creator_earnings_70": 77.00,
  "artist_raise_30": 33.00,
  "total_revenue": 110.00
}
```
