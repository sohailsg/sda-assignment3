"""Stream sample_data.csv to the `raw-voltage` Kafka topic, one message per frame.

A frame is a packet_id's worth of consecutive ticks (FRAME_TICKS ticks) for one
lead group, carrying arrays of samples per channel instead of one message per
sample.
"""

import argparse
import csv
import json
import time

from kafka import KafkaProducer
from kafka.serializer import Serializer

BOOTSTRAP = "localhost:9092"
TOPIC = "raw-voltage"
SIGNAL_CSV = "sample_data.csv"
META_CSV = "channels_metadata.csv"

FS = 250                       # device sampling rate (ticks/sec)
FRAME_TICKS = 8                # packet_id groups this many ticks (32 ms @ 250 Hz)

STATIC_KEYS = ("channel_name", "lead_group", "units", "sampling_frequency_hz",
               "reference_scheme", "gain_uV_per_lsb", "zero_digital",
               "low_cutoff_hz", "high_cutoff_hz", "anatomical_location")


class Utf8KeySerializer(Serializer):
    def serialize(self, topic, headers, key):
        return key.encode("utf-8") if key is not None else None


class JsonValueSerializer(Serializer):
    def serialize(self, topic, headers, value):
        return json.dumps(value).encode("utf-8")


def load_metadata(path):
    with open(path, newline="") as f:
        return {r["channel_id"]: {k: r[k] for k in STATIC_KEYS}
                for r in csv.DictReader(f)}


class FrameBuilder:
    """Accumulates rows for one (patient, epoch, lead_group, packet_id)."""

    def __init__(self, row, meta):
        self.patient_id = row["patient_id"]
        self.epoch_id = row["epoch_id"]
        self.lead_group = meta[row["channel_id"]]["lead_group"]
        self.packet_id = int(row["packet_id"])
        self.partition_key = row["partition_key"]
        self.channels = {}
        self.add(row, meta)

    def matches(self, row):
        return (row["patient_id"] == self.patient_id
                and row["epoch_id"] == self.epoch_id
                and int(row["packet_id"]) == self.packet_id)

    def add(self, row, meta):
        cid = row["channel_id"]
        ch = self.channels.setdefault(cid, {"meta": meta.get(cid, {}), "samples": []})
        ch["samples"].append({
            "seq": int(row["seq"]),
            "timestamp_ms": int(row["timestamp_ms"]),
            "sample_value_digital": int(row["sample_value_digital"]),
            "physical_value_uV": float(row["physical_value_uV"]),
            "channel_status": row["channel_status"],
            "status_reason": row["status_reason"],
            "stim_active": row["stim_active"] == "True",
            "signal_state": row["signal_state"],
        })

    def build(self):
        all_samples = [s for ch in self.channels.values() for s in ch["samples"]]
        seqs = {s["seq"] for s in all_samples}
        frame = {
            "patient_id": self.patient_id,
            "epoch_id": self.epoch_id,
            "lead_group": self.lead_group,
            "packet_id": self.packet_id,
            "frame_start_ms": min(s["timestamp_ms"] for s in all_samples),
            "seq_start": min(seqs),
            "n_samples": len(seqs),
            "channels": {},
        }
        for cid, ch in self.channels.items():
            samples = sorted(ch["samples"], key=lambda s: s["seq"])
            stim = [s["stim_active"] for s in samples]
            frame["channels"][cid] = {
                **ch["meta"],
                "sample_value_digital": [s["sample_value_digital"] for s in samples],
                "physical_value_uV": [s["physical_value_uV"] for s in samples],
                "channel_status": samples[0]["channel_status"],
                "status_reason": samples[0]["status_reason"],
                "signal_state": samples[0]["signal_state"],
                "stim_active": stim[0] if len(set(stim)) == 1 else stim,
            }
        return frame


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=BOOTSTRAP)
    ap.add_argument("--topic", default=TOPIC)
    ap.add_argument("--signal", default=SIGNAL_CSV)
    ap.add_argument("--meta", default=META_CSV)
    ap.add_argument("--limit", type=int, default=0, help="0 = read all rows")
    ap.add_argument("--rate", type=float, default=250,
                     help="ticks/sec; 250 = real-time device rate "
                          f"({FRAME_TICKS} ticks / {FRAME_TICKS * 1000 / 250:.0f} ms per frame), "
                          "0 = max speed")
    args = ap.parse_args()

    meta = load_metadata(args.meta)

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

    frame_gap = FRAME_TICKS / args.rate if args.rate else 0
    print(f"streaming {args.signal} -> topic '{args.topic}' at "
          f"{'max speed' if not args.rate else f'{args.rate:g} ticks/s'} "
          f"({FRAME_TICKS} ticks/frame, {len(meta)} channels)")

    buffers = {}            # lead_group -> FrameBuilder
    frames_sent = 0
    rows_seen = 0
    packets_seen = set()    # (patient_id, epoch_id, packet_id)
    last_packet_key = None
    t_start = time.time()

    def flush(buf):
        nonlocal frames_sent
        frame = buf.build()
        producer.send(
            args.topic,
            key=buf.partition_key,
            value=frame,
            timestamp_ms=frame["frame_start_ms"],
        )
        frames_sent += 1

    with open(args.signal, newline="") as f:
        for row in csv.DictReader(f):
            rows_seen += 1
            packet_key = (row["patient_id"], row["epoch_id"], int(row["packet_id"]))

            if frame_gap and packet_key != last_packet_key:
                if last_packet_key is not None:
                    time.sleep(frame_gap)
                last_packet_key = packet_key

            lead_group = meta[row["channel_id"]]["lead_group"]
            buf = buffers.get(lead_group)
            if buf is None or not buf.matches(row):
                if buf is not None:
                    flush(buf)
                buf = FrameBuilder(row, meta)
                buffers[lead_group] = buf
            else:
                buf.add(row, meta)

            point = {
                "record_id": row["record_id"],
                "channel_id": row["channel_id"],
                "seq": int(row["seq"]),
                "packet_id": int(row["packet_id"]),
                "sample_value_digital": int(row["sample_value_digital"]),
                "physical_value_uV": float(row["physical_value_uV"]),
            }
            print(f"Sent: {json.dumps(point)}")

            if packet_key not in packets_seen:
                packets_seen.add(packet_key)
                if len(packets_seen) % 5_000 == 0:
                    el = time.time() - t_start
                    sig_s = len(packets_seen) * FRAME_TICKS / FS
                    print(f"sent {frames_sent:,} frames | {sig_s:,.1f}s of signal | {el:.1f}s elapsed")

            if args.limit and rows_seen >= args.limit:
                break

    for buf in buffers.values():
        flush(buf)

    producer.flush()
    producer.close()
    el = time.time() - t_start
    sig_s = len(packets_seen) * FRAME_TICKS / FS
    print(f"done: {frames_sent:,} frames ({rows_seen:,} samples) -> {args.topic} "
          f"in {el:.1f}s ({sig_s:,.1f}s of signal)")


if __name__ == "__main__":
    main()
