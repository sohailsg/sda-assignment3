# Neuro-Surge Monitoring Pipeline — Assignment 3

Real-time pipeline that streams simulated implanted-electrode (RNS-320) brain
activity and device telemetry through Kafka, scores every reading with a
seizure-detection formula, stores the results in MySQL, and visualizes them
in Grafana.

```
producer → Kafka (raw-voltage) ─┐
                                  ├─→ consumer.py → MySQL → Grafana dashboards
device_health_producer.py →     ┘
   Kafka (device-health)
```

## Folder structure

```
assignment 3/
├── README.md                          this file
├── requirements.txt                   pip dependencies
├── channels_metadata.csv              static per-electrode metadata (input to both producer and consumer)
├── producer.py                        Kafka PRODUCER #1 -- streams the raw-voltage waveform topic
├── device_health_producer.py          Kafka PRODUCER #2 -- streams the device-health telemetry topic
├── consumer.py                        Kafka CONSUMER -- scores every frame and writes to MySQL
├── pipeline/                          the scoring logic consumer.py imports (kept separate so it's unit-testable)
│   ├── config.py                      every threshold/multiplier used by the formula (see below)
│   ├── features.py                    signal-feature math (line length, band power, rolling baseline)
│   ├── alert_rules.py                 the Surge Index formula + severity classification
│   └── episodes.py                    groups consecutive elevated frames into one discrete alert episode
├── sql/
│   ├── schema.sql                     MySQL table/view definitions (run this first)
│   └── dashboard_queries.sql          every SQL query behind every dashboard panel, documented
└── dashboard/
    ├── dashA_realtime_clinical.json   Grafana dashboard: on-call / live view
    └── dashB_longitudinal_review.json Grafana dashboard: week-over-week clinical review
```

## Setup

1. **Infrastructure**: a running Kafka broker and a MySQL instance. Any Kafka
   image works; MySQL needs to be 8.0+ (uses window-function-free CTEs are
   not required, but `JSON`-free schema needs plain MySQL 8).
2. **Python deps**: `pip install -r requirements.txt`
3. **Kafka topics** (create once):
   ```
   kafka-topics.sh --create --topic raw-voltage    --partitions 2 --replication-factor 1
   kafka-topics.sh --create --topic device-health  --partitions 2 --replication-factor 1
   ```
4. **Database schema**: `mysql -u<user> -p<pass> <db> < sql/schema.sql`
5. **Config**: all connection settings are environment variables (see
   `pipeline/config.py`), with sane localhost defaults:
   `KAFKA_BOOTSTRAP`, `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`,
   `MYSQL_PASSWORD`, `MYSQL_DB`.

## Running it

```bash
# 1. Start the consumer first (it waits on Kafka)
python consumer.py

# 2. Stream waveform data (one lead group per Kafka message, 250 Hz device rate)
python producer.py --rate 250          # --rate 0 = max speed, for backfilling history

# 3. Stream device telemetry (impedance + stimulation current, ~1 msg/sec/channel)
python device_health_producer.py --duration 60
```

`consumer.py` joins the two `raw-voltage` lead-group messages for the same
packet before scoring (the formula needs all 4 channels together to tell a
focal seizure from a whole-head artifact — see below), evaluates the formula,
tracks discrete alert episodes, and writes everything to MySQL inside the
same transaction it commits the Kafka offset in, so a crash/retry can't
double-count a frame.

## Import the dashboards

In Grafana: **Connections → Data sources → Add data source → MySQL**, point
it at the database `sql/schema.sql` was run against, then **Dashboards →
Import** each JSON file in `dashboard/`. On import, Grafana will ask you to
map the dashboard's datasource placeholder to the MySQL data source you just
created — select it for both dashboards.

---

## The mathematical formula

Every 32ms frame, for every patient, the consumer computes a single number —
the **Surge Index (SI)** — that scores how much that moment looks like a real
seizure, and classifies it into a severity.

### 1. Per-channel features (`pipeline/features.py`)

For each channel's most recent 128-sample (≈0.5s) window:

- **Line length**: `sum(|x[i] - x[i-1]|)` — amplitude × frequency in one
  number; the same statistic the real RNS device itself uses.
- **`ll_ratio`**: that channel's line length divided by its own rolling
  median baseline (learned only from *clean* windows — a channel that's
  currently bad or already elevated doesn't get to poison its own baseline).
- **`band_ratio`**: fraction of spectral power in 13–30Hz vs the full 4–90Hz
  sensing band — real seizures are rhythmic; a movement/EMG artifact is
  broadband noise, so this number is what tells the two apart.

### 2. The Surge Index (`pipeline/alert_rules.score_surge`)

```
A          = max(ll_ratio_c for c in good_channels)      # amplitude term
c*         = the channel that produced A
n_elevated = count of good channels with ll_ratio >= 2.5

M_spatial  = 1.0   if n_elevated <= 2                     # focal -> not diluted
             0.3   if n_elevated >= 3 AND n_elevated == n_good   # every good
                                                            # channel elevated at
                                                            # once -> looks like a
                                                            # whole-head artifact,
                                                            # not a focal seizure
             0.7   otherwise

M_soz      = 1.2   if c* is the known seizure-onset-zone channel
             1.0   otherwise

M_rhythm   = 1.0   if band_ratio(c*) >= 0.40              # rhythmic, seizure-shaped
             0.5   otherwise                               # broadband, artifact-shaped

Surge Index (SI) = A * M_spatial * M_soz * M_rhythm
```

**Why `max()` across channels instead of an average**: a real focal seizure
on one electrode, averaged in with three quiet electrodes, gets diluted below
any sane threshold. `max()` preserves the signal a focal event actually
produces; the spatial multiplier is what then tells a genuine focal surge
apart from a global artifact — a weighted average cannot express either of
those ideas at once.

### 3. Duration

`duration_ms` = length of the current run of consecutive frames with
`SI >= 2.5` on the leading channel. This is what separates a brief
sub-clinical blip from a sustained event.

### 4. Severity classification (`pipeline/alert_rules.classify`)

Three **independent rule families** are evaluated, each producing its own
severity; the **worst severity across all three wins** — never averaged,
because a device fault shouldn't be diluted by an otherwise-quiet brain, and
vice versa.

| Family | Critical | Warning | Alert type |
|---|---|---|---|
| **Seizure** | `SI >= 4.0` **and** `duration_ms >= 400` | `SI >= 2.5` | `SEIZURE_SURGE` (or `SUBCLINICAL` if never reaching 4.0) |
| **Signal quality** | saturation on the seizure-onset channel during an active seizure | any channel flagged bad (saturated / broken / line-noise) | `SIGNAL_QUALITY` |
| **Device safety** | stimulation charge density > 25 µC/cm² (the Shannon-limit hard cap) | electrode impedance outside 250–3500 Ω | `DEVICE_SAFETY` |

If the amplitude term `A` is high (>= 4.0) but `M_spatial` says the surge is
spread across every good channel, the frame is classified as `ARTIFACT` and
kept at `normal` severity — this is what stops a whole-head movement burst
from ever being mistaken for a seizure, regardless of how large it looks.

The 400ms critical-duration floor is a compressed-time equivalent of the RNS
device's own 30-second "long episode" rule (this dataset compresses a ~59s
real seizure into ~1.26s so a full arc fits in one short recording).

### 5. Episodes, not frames

A single seizure produces dozens of consecutive scored frames. `pipeline/episodes.py`
groups them into one **episode** per event (start time, end time, peak SI,
onset channel, channels it spread to, whether stimulation fired), closing an
episode after 5 consecutive normal frames. All of the dashboard panels that
count or trend "seizures" query `alert_episodes`, not raw frames — counting
frames would inflate every number by however many frames one event happened
to span.

### 6. Device safety (independent stream)

`device_health_producer.py` streams electrode impedance and programmed
stimulation current. `consumer.py` converts current into charge density
(`current_mA * pulse_width_s * 1000 / electrode_area_cm2`) and checks it
against the same two hard limits every reading, independent of whatever the
brain signal is doing.
