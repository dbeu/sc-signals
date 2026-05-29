# SC Signals

Production signal-only live v1 for the short-small-caps strategies.

## Current Version

- Repo: `git@github.com:dbeu/sc-signals.git`
- Live version: signal-only v1
- VPS: `45.76.19.162`
- Stage 2 service: `sc-signals.service`
- Stage 2 endpoint: `http://45.76.19.162:8080/events`
- Health endpoint: `http://45.76.19.162:8080/health`

This system sends Discord signal alerts only. It does not place orders, manage
locates, or execute trades.

## Architecture

Stage 1 runs on the machine with Polygon/Massive access. It fetches and
normalizes market data, writes local event directories, and posts each event to
Stage 2.

Stage 2 runs on the VPS. It receives events, stores them, computes signals,
saves signal files, and sends Discord notifications. Stage 2 never calls
Polygon/Massive and does not talk back to Stage 1.

Event flow:

```text
Polygon/Massive -> Stage 1 -> HTTP /events -> Stage 2 -> Discord
```

## Strategies

The deployed strategy set is:

- `GO`: open-entry gap/selloff signal
- `D2O`: day-2 open-entry signal
- `GE`: gap-extension context with RE-style entry
- `RE`: regular extension signal
- `D2E`: day-2 extension signal

All strategies use the same signed premarket selloff convention:

```text
pm_selloff = open / premarket_high - 1
```

`pm_selloff` is negative when the regular-session open is below the premarket
high.

Open-entry behavior:

- `GO` and `D2O` can alert before the open with `signal_phase=preopen`.
- The pre-open `entry` is an estimate from latest snapshot/premarket context.
- The strategy time remains `09:30:00`.
- Stage 2 de-duplicates by `strategy/ticker/date/time`, so pre-open alerts are
  not repeated again at the open.

## Production Schedule

Stage 1 runs continuously across days with `--loop`.

- Start sending: `09:20:00` ET
- Stop sending: `14:05:00` ET
- Poll interval: `60` seconds
- Local Stage 1 retention: `7` days
- VPS event retention: `14` days
- VPS signal retention: `90` days

Why stop at `14:05 ET`:

- `GO`: open only
- `D2O`: open only
- `GE`: `09:30-10:59`
- `RE`: `11:00-13:59`
- `D2E`: `before_1400`
- 5-minute bars need the `14:00-14:05` processing boundary for the last
  `before_1400` entry check.

Stage 1 uses a weekday check only. Exchange holidays should be monitored
manually for live v1.

## Launch Stage 1

Run from the local machine:

```bash
cd /home/daniel/Documents/codebox/algotrading/sc-signals

python3 stage1_polygon_fetcher.py \
  --loop \
  --out-dir stage1_events \
  --post-url http://45.76.19.162:8080/events \
  --start-time 09:20:00 \
  --stop-time 14:05:00 \
  --poll-seconds 60 \
  --reference-limit 0
```

`--loop` is multi-day by default. Stage 1 keeps running, sleeps outside the
live window, rebuilds context each weekday, and starts the next trade date
automatically.

Use `--single-day` only for testing/debugging.

## Stage 1 Fetch Logic

At the start of each trade date, Stage 1:

- fetches all active US stock reference pages when `--reference-limit 0`
- filters likely non-common tickers
- fetches grouped daily data for the previous trading day
- initializes a fresh day archive under `stage1_events/YYYY-MM-DD/`

Every poll cycle, Stage 1:

- fetches the market snapshot
- builds daily/snapshot context
- applies cheap route filters
- fetches current-day minute bars for candidate tickers
- routes tickers for `GO`, `GE`, `RE`, and `D2`
- fetches previous-day extended-hours bars for newly D2-routed tickers
- fetches current-day minute bars for all active routed tickers
- sends only bar deltas newer than the last sent bar per ticker
- posts one event directory to Stage 2

Stage 1 does not compute final signals and does not send Discord messages.

## Stage 2 Behavior

Stage 2 receives each event and:

- saves it under `/opt/sc_stage2_inbox`
- logs every request to `/opt/sc_stage2_access.jsonl`
- detects the event trade date
- resets in-memory signal state when the trade date changes
- processes bars/context into signals
- sends new Discord signals
- writes signal files under `/opt/sc_stage2_signals/YYYY-MM-DD/`
- prunes memory to current date plus needed previous-date bars

Health check:

```bash
curl http://45.76.19.162:8080/health
```

Public health only returns:

```json
{"ok": true}
```

Detailed health requires the Stage 1 bearer token:

```bash
curl -H "Authorization: Bearer $SC_STAGE1_TOKEN" http://45.76.19.162:8080/health
```

Detailed health output includes:

- `discord_enabled`
- `last_event_utc`
- `trade_date`
- `signals_generated`
- memory stats: context rows, bar rows, and MB

## Data Needed For D2

D2O/D2E need:

- previous trading day `04:00-20:00` minute bars
- current day premarket bars
- current day regular-session bars as they arrive
- daily context:
  - `prev_date`
  - `prev_open`
  - `prev_high`
  - `prev_low`
  - `prev_close`
  - `prev_volume`

Stage 1 sends previous-day extended-hours bars when a ticker is D2-routed.
Stage 2 keeps those previous-day bars in memory for the active trade date.

## Stale Data Alerts

Stage 2 watches for missing Stage 1 data during `09:20-14:05 ET`.

If no event arrives for `180` seconds, Stage 2 sends a Discord warning. It
repeats stale warnings no more often than about every five minutes while the
problem persists.

This does not require Stage 2 to talk to Stage 1.

## VPS Service

Install/update the service:

```bash
cd /root/sc-signals
git pull
cp deploy/sc-signals.service /etc/systemd/system/sc-signals.service
systemctl daemon-reload
systemctl enable sc-signals
systemctl restart sc-signals
systemctl status sc-signals --no-pager -l
```

Logs:

```bash
journalctl -u sc-signals -f
```

Access log:

```bash
tail -f /opt/sc_stage2_access.jsonl
```

Each access-log line is JSON with the request timestamp, client IP, method,
path, status, and outcome. Accepted `/events` requests also include event name,
trade date, bytes received, and number of new signals. Unauthorized requests are
logged with `outcome="rejected"` and `detail="unauthorized"`.

The receiver also hides detailed health from unauthenticated callers, uses a
generic HTTP server fingerprint, caps request bodies at 100 MB, rejects unknown
uploaded file names, and rate-limits repeated bad `/events` requests from the
same IP.

The service uses:

```text
/opt/sc_stage2_inbox
/opt/sc_stage2_signals
/opt/sc_stage2_access.jsonl
```

Retention:

- event inbox: `14` days
- signal outputs: `90` days

## Local Setup

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Required `.env` values:

```text
MASSIVE_API_KEY=
SC_STAGE1_TOKEN=
DISCORD_TOKEN=
DISCORD_CHANNEL_ID=
```

`DISCORD_TOKEN` and `DISCORD_CHANNEL_ID` are required only on the VPS receiver
for Discord alerts. `MASSIVE_API_KEY` is required only where Stage 1 runs.

## Test Commands

Discord smoke test on the VPS:

```bash
cd /root/sc-signals
. .venv/bin/activate
python3 test_discord.py --message "SC Signals Discord smoke test"
```

Historical replay into a running receiver:

```bash
python3 simulate_replay_day_to_server.py \
  --date 2026-01-30 \
  --tickers FEED \
  --out-dir /tmp/sc_signals_replay_feed \
  --url http://45.76.19.162:8080/events \
  --clean
```

Known replay checks:

- `FEED 2026-01-30`: expected 4 signals (`D2O` x1, `D2E` x3)
- `BVC 2026-01-05`: expected 1 `GO` signal
- `PRZO 2026-01-05`: expected 1 `D2O` signal

## Operational Notes

- Keep the Stage 1 machine awake and online.
- Rotate the VPS root password if it was shared during setup.
- Prefer SSH keys over password SSH.
- Watch `/opt/sc_stage2_access.jsonl` for unexpected client IPs or repeated
  unauthorized requests.
- On the first live day, watch logs from `09:20` through at least `09:35 ET`.
- Live v1 is signal-only. Manual trade review/execution remains outside this
  repo.
