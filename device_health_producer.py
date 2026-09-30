"""Streams simulated implant device-health telemetry (electrode impedance +
stimulation current) to the `device-health` Kafka topic.

Separate producer from producer.py: different source (telemetry vs. raw
waveform), different rate (1 msg/s per channel vs ~31 frames/s per lead),
and independent, non-redundant features (Phase 1.5 in
docs/IMPLEMENTATION_GUIDE.md).
"""

import argparse
import csv
import json
import time

import numpy as np
from kafka import KafkaProducer

from pipeline.config import DEVICE_TOPIC, BOOTSTRAP
from producer import JsonValueSerializer, Utf8KeySerializer

META_CSV = "channels_metadata.csv"


def load_channels(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=BOOTSTRAP)
    ap.add_argument("--topic", default=DEVICE_TOPIC)
    ap.add_argument("--meta", default=META_CSV)
    ap.add_argument("--patients", default="PT-0042,PT-0117,PT-0233,PT-0356,PT-0471")
    ap.add_argument("--interval", type=float, default=1.0, help="seconds between telemetry rounds")
    ap.add_argument("--duration", type=float, default=60.0, help="seconds to run live, 0 = forever")
    ap.add_argument("--backfill-minutes", type=float, default=0,
                     help="emit this many minutes of past telemetry at max speed, then exit")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--fault-patient", default=None,
                     help="patient to inject an over-limit stim fault on (ch2, middle third of the run)")
    args = ap.parse_args()

    rng = np.random.default_rng(args.seed)
    channels = load_channels(args.meta)
    patients = args.patients.split(",")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        key_serializer=Utf8KeySerializer(),
        value_serializer=JsonValueSerializer(),
        acks="all",
        retries=5,
        linger_ms=20,
        batch_size=64 * 1024,
        compression_type="gzip",
    )

    # per (patient, channel) state: impedance random walk from the metadata value,
    # programmed stim current 1-4 mA
    state = {
        (p, row["channel_id"]): {
            "impedance_ohm": float(row["impedance_ohm"]) + rng.normal(0, 60),
            "stim_current_mA": float(rng.uniform(1.0, 4.0)),
        }
        for p in patients for row in channels
    }

    if args.backfill_minutes:
        n_rounds = int(args.backfill_minutes * 60 / args.interval)
        t0_ms = int(time.time() * 1000) - int(args.backfill_minutes * 60_000)
    else:
        n_rounds = int(args.duration / args.interval) if args.duration else 10 ** 12
        t0_ms = None

    print(f"device health -> '{args.topic}': {len(patients)} patients x {len(channels)} channels, "
          f"{n_rounds} rounds every {args.interval}s{' (backfill)' if t0_ms else ''}")

    n_sent = 0
    for r in range(n_rounds):
        now_ms = t0_ms + int(r * args.interval * 1000) if t0_ms else int(time.time() * 1000)
        in_fault_window = n_rounds // 3 <= r < 2 * n_rounds // 3

        for p in patients:
            for row in channels:
                cid = row["channel_id"]
                s = state[(p, cid)]

                # bounded random walk, sigma=5 ohm/round, clipped [200, 6000]
                s["impedance_ohm"] = float(np.clip(s["impedance_ohm"] + rng.normal(0, 5), 200, 6000))
                # rare re-programming of stim current, normally 1-4 mA
                if rng.uniform() < 0.005:
                    s["stim_current_mA"] = float(rng.uniform(1.0, 4.0))

                current = s["stim_current_mA"]
                if p == args.fault_patient and cid == "ch2" and in_fault_window:
                    current = 14.0  # deliberate over-limit programming fault (-> ~28 uC/cm2)

                msg = {
                    "patient_id": p,
                    "channel_id": cid,
                    "event_ms": now_ms,
                    "impedance_ohm": round(s["impedance_ohm"], 1),
                    "stim_current_mA": round(current, 2),
                }
                producer.send(args.topic, key=p, value=msg, timestamp_ms=now_ms)
                n_sent += 1

        if not t0_ms:
            producer.flush()
            time.sleep(args.interval)

    producer.flush()
    producer.close()
    print(f"done: {n_sent} device-health messages sent")


if __name__ == "__main__":
    main()
