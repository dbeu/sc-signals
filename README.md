# SC Signals

Live Stage 1/Stage 2 signal transport for the short-small-caps system.

Stage 1 fetches/normalizes Polygon/Massive data and writes event directories.
Stage 2 receives those event directories, stores them, and can process them
with the signal engine. The stages are independent; Stage 2 never calls
Polygon.

## Files

- `stage1_polygon_fetcher.py`: fetches a seed event from Polygon/Massive.
- `stage2_event_receiver.py`: simple HTTP receiver with `/health` and `/events`.
- `stage2_signal_engine.py`: reads event directories and generates signals.
- `send_event_dir.py`: posts an existing event directory to the receiver.
- `event_transport.py`: JSON/base64 event packing and POST helper.
- `config/selected_params.csv`: robust-v3 clean-common strategy parameters.

## Local Receiver

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Start Stage 2 receiver:

```bash
SC_STAGE1_TOKEN=dev-test-token \
python3 stage2_event_receiver.py \
  --host 0.0.0.0 \
  --port 8080 \
  --inbox-dir stage2_inbox \
  --signals-dir stage2_signals
```

Health check:

```bash
curl http://127.0.0.1:8080/health
```

## Send A Test Event

Fetch one small Stage 1 event and POST it to the receiver:

```bash
SC_STAGE1_TOKEN=dev-test-token \
python3 stage1_polygon_fetcher.py \
  --date 2026-01-30 \
  --prev-date 2026-01-29 \
  --tickers FEED \
  --max-bar-tickers 1 \
  --out-dir stage1_events/feed_seed \
  --post-url http://127.0.0.1:8080/events
```

Or send an event directory that already exists:

```bash
SC_STAGE1_TOKEN=dev-test-token \
python3 send_event_dir.py \
  --event-dir stage1_events/feed_seed/0000_seed \
  --url http://127.0.0.1:8080/events
```

Historical day replay into a running receiver:

```bash
python3 simulate_replay_day_to_server.py \
  --date 2026-01-30 \
  --tickers FEED \
  --out-dir stage1_events/replay_2026-01-30_feed \
  --url http://127.0.0.1:8080/events \
  --clean
```

## Run Stage 2 Locally

```bash
python3 stage2_signal_engine.py \
  --events-dir stage1_events/feed_seed \
  --out-dir stage2_signals/feed_seed
```

## VPS Start Command

```bash
SC_STAGE1_TOKEN='replace-with-a-long-random-token' \
python3 stage2_event_receiver.py \
  --host 0.0.0.0 \
  --port 8080 \
  --inbox-dir /opt/sc_stage2_inbox \
  --signals-dir /opt/sc_stage2_signals
```

If `.env` contains `DISCORD_TOKEN` and `DISCORD_CHANNEL_ID`, the receiver sends
a Discord batch for every POST that creates new signals. Use
`--dry-run-discord` to print notification text without sending it.

All strategies use the same premarket selloff convention:

```text
pm_selloff = open / premarket_high - 1
```

So `pm_selloff` is negative when the regular-session open is below the
premarket high.

## Production Schedule

Live v1 Stage 1 should run on market days with this window:

- Start posting: `09:20:00` ET. This gives GO/D2O open-entry strategies
  pre-open alerts using the latest snapshot/premarket price as an estimated
  open.
- Stop posting: `14:05:00` ET. The deployed strategies do not need later data:
  GO/D2O fire at open, GE is morning, RE is midday, and D2E uses the
  `before_1400` window.
- Poll interval: `60` seconds.
- Local Stage 1 retention: `7` days.
- VPS event retention: `14` days.
- VPS signal retention: `90` days.

Run the production Stage 1 loop from the machine with Polygon/Massive access:

```bash
python3 stage1_polygon_fetcher.py \
  --loop \
  --out-dir stage1_events \
  --post-url http://45.76.19.162:8080/events \
  --start-time 09:20:00 \
  --stop-time 14:05:00 \
  --poll-seconds 60 \
  --reference-limit 0
```

`--reference-limit 0` means fetch all active US stock reference pages once at
startup. Stage 1 then polls the all-market snapshot endpoint once per cycle,
routes likely tickers, fetches minute bars only for routed/active tickers, and
posts each event cycle to Stage 2.

With `--loop`, Stage 1 keeps running across days by default. Each weekday it
rebuilds the reference universe and previous-day context for the new trade
date, runs the `09:20-14:05 ET` loop, cleans old local archives, then sleeps
until the next day. It uses a weekday check only; exchange holidays should be
monitored manually for live v1. Use `--single-day` only for testing/debugging.

Open-entry alerts:

- `GO` and `D2O` can alert before the open with `signal_phase=preopen`.
- The pre-open `entry` is an estimate from the latest snapshot/premarket
  context.
- The actual strategy time remains `09:30:00`.
- Stage 2 de-duplicates by `strategy/ticker/date/time`, so the pre-open alert
  is not repeated at the open for the same strategy/ticker.

## Systemd

Install the VPS receiver as a service:

```bash
cp deploy/sc-signals.service /etc/systemd/system/sc-signals.service
systemctl daemon-reload
systemctl enable sc-signals
systemctl start sc-signals
systemctl status sc-signals
```

Logs:

```bash
journalctl -u sc-signals -f
```

Stage 2 also watches for missing Stage 1 data during the live window
(`09:20-14:05 ET`). If no event arrives for `180` seconds, it sends a Discord
warning. It repeats stale warnings no more often than roughly every five
minutes while the issue persists.
