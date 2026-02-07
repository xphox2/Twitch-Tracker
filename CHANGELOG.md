# Changelog

All notable changes to this project are documented in this file.

## v0.0.9 (Unreleased)

### Tracking
- Fix subscriber count to use Helix `/subscriptions` `total` (previously capped at the first page).
- Fix subscriber identity to use Helix `user_id` (previously used a non-existent `id` field), which stabilizes subscriber sync and history.
- Fix subscriber sync logic so ended subs are not repeatedly re-added as "ended" on every poll; resubs are detected.
- Fix subscription revenue math to avoid double-counting gifted subs in the artist fund.
- Split subscription revenue into disjoint buckets (`nongift_subs_revenue` and `gifted_subs_revenue`) and sum once.
- Add backwards-compatible migration behavior for existing `data/sub_tiers.json` so new revenue buckets don't create a one-time "daily" spike.

### Bits
- Replace incorrect Helix bits leaderboard usage with real-time cheer tracking via Twitch IRC tags.
- Add `data/bits_tracker.json` to track today/lifetime bits revenue (USD) and keep it consistent across restarts.
- Add CSV import seeding for lifetime bits revenue (`lifetime_bits_revenue_imported`) and combine with live-tracked bits revenue.
- Add `--simulate-cheer <bits>` for local testing without going live.

### API / Auth
- Update OAuth scopes to match required endpoints: add `moderator:read:followers` and `chat:read`; remove unused `bits:read`.
- Improve followers error messaging when scope/token is missing.

### Reliability
- Use atomic writes (`.tmp` + `os.replace`) for JSON files read by OBS/browser sources to prevent partial JSON reads.
- Add locks around bits tracker and artist earnings updates (polling loop + background IRC thread).

### Overlays / HTML / Branding
- Add cache-busting and `no-store` fetch options across overlays to reduce OBS caching/stale stats issues.
- Update overlays that display bits to treat `bits_today`/`bits_total` as USD and format with `$` + 2 decimals.
- Fix `obs_overlay.html` monthly artist calculation to include gifted totals.
- Fix `admin_stats.html` tier price display mapping for Tier 2/3.
- Add `<meta charset="utf-8">` to `branding.html` to prevent mojibake for emoji characters.
- Add compatibility mapping in `branding-setup.js` so older templates that use generic CSS variables still reflect modular branding configuration.
