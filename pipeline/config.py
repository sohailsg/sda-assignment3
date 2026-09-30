"""Central configuration for the surge-alerting pipeline.

All thresholds/multipliers from DATASET_RULES.md-compatible research (see
docs/IMPLEMENTATION_GUIDE.md Phase 3). Override via env vars for deployment
without touching code.
"""

import os

# ---------------------------------------------------------------- Kafka
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
VOLTAGE_TOPIC = os.environ.get("VOLTAGE_TOPIC", "raw-voltage")
DEVICE_TOPIC = os.environ.get("DEVICE_TOPIC", "device-health")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "surge-alerting")
AUTO_OFFSET_RESET = os.environ.get("AUTO_OFFSET_RESET", "earliest")

# ---------------------------------------------------------------- MySQL
# NOTE: docker-compose.yml (untouched) provisions db "stockdb" on host port
# 3307 with root/root. We reuse that instance/db rather than renaming it.
MYSQL_HOST = os.environ.get("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3307"))
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "root")
MYSQL_DB = os.environ.get("MYSQL_DB", "stockdb")

# ---------------------------------------------------------------- device constants
FS = 250                     # Hz, matches channels_metadata.csv
FRAME_TICKS = 8              # ticks per frame/packet (32 ms)
CHANNEL_IDS = ("ch1", "ch2", "ch3", "ch4")
SOZ_CHANNELS = {"ch1"}       # is_seizure_onset_zone == True in channels_metadata.csv

# ---------------------------------------------------------------- feature windows
WINDOW_SAMPLES = 128         # samples used for line-length / band-power window
BASELINE_WARMUP_WINDOWS = 250  # rolling median window count for per-channel baseline
BASELINE_MIN_WINDOWS = 30      # windows needed before a channel is scored (~1 s)

# ---------------------------------------------------------------- surge formula thresholds
# Compressed-time scale: dataset compresses a real ~59s seizure into ~1.26s
# (DATASET_RULES.md Sec.5), a factor of ~47x. RNS "long episode" = 30-60s real
# time -> ~640-1275ms compressed. We use 400ms as the compressed critical-
# duration floor (conservative, slightly below the compressed 30s bound).
LL_RATIO_ELEVATED = 2.5      # per-channel line-length ratio counted as "elevated"
SI_WARNING = 2.5             # surge index >= this -> at least WARNING
SI_CRITICAL = 4.0            # surge index >= this (+duration) -> CRITICAL
CRITICAL_DURATION_MS = 400   # consecutive elevated time required for CRITICAL
BAND_RATIO_RHYTHMIC = 0.40   # 13-30Hz / 4-90Hz power ratio considered "rhythmic"

M_SPATIAL_FOCAL = 1.0        # n_elevated <= 2
M_SPATIAL_GLOBAL = 0.3       # n_elevated >= 3 and == n_good (looks like artifact)
M_SPATIAL_MIXED = 0.7        # otherwise
M_SOZ_BONUS = 1.2            # argmax channel is a seizure-onset-zone channel
M_RHYTHM_LOW = 0.5           # band_ratio < BAND_RATIO_RHYTHMIC

# ---------------------------------------------------------------- device safety
IMPEDANCE_MIN_OHM = 250
IMPEDANCE_MAX_OHM = 3500
ELECTRODE_AREA_CM2 = 0.079        # ~7.9 mm^2 (channels_metadata.csv size_mm2)
PULSE_WIDTH_S = 160e-6            # 160 us, RNS default
CHARGE_DENSITY_CRITICAL_UC_CM2 = 25.0  # Shannon limit / RNS hard cap

# ---------------------------------------------------------------- episode closing
EPISODE_CLOSE_NORMAL_FRAMES = 5   # consecutive normal frames to close an open episode
