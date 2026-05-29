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
