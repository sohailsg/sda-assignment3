"""Kafka consumer: reads raw-voltage (waveform frames) and device-health
(telemetry), applies the surge-alert formula per frame, tracks episodes, and
writes results to MySQL. See README.md for the full pipeline description and
the mathematical formula.

Business rules applied here (beyond the pure formula in pipeline/):
- The line-length baseline is per (patient, channel) and persists across
  epochs. A patient's normal background doesn't reset between recordings.
- A channel's baseline only learns from windows where that channel is not
  elevated. Otherwise a seizure would raise its own baseline and the surge
  index would decay while the seizure is still going.
- A channel isn't scored until its baseline has BASELINE_MIN_WINDOWS clean
  windows, so there are no alerts off a half-learned baseline.
- Seizure, signal-quality and device families are stored and episoded
  separately (see alert_rules.classify_families).
"""

import argparse
import csv
import json
from collections import defaultdict

import mysql.connector
from kafka import KafkaConsumer

from pipeline import config
from pipeline.alert_rules import DeviceState, classify, classify_device, classify_families, score_surge
from pipeline.episodes import EpisodeTracker
from pipeline.features import RobustBaseline, band_ratio, line_length

META_CSV = "channels_metadata.csv"
_STATE_RANK = {"interictal": 0, "post-ictal": 1, "pre-ictal": 2, "ictal": 3}


def load_metadata(path):
    with open(path, newline="") as f:
        return {r["channel_id"]: r for r in csv.DictReader(f)}


def get_connection():
    return mysql.connector.connect(
        host=config.MYSQL_HOST, port=config.MYSQL_PORT,
        user=config.MYSQL_USER, password=config.MYSQL_PASSWORD,
        database=config.MYSQL_DB,
    )


class ChannelBuffer:
    """Per-(patient, epoch, channel) ring buffer keyed by seq. Handles
    duplicate/out-of-order/gap detection (E4/E5/E6)."""

    def __init__(self, maxlen=config.WINDOW_SAMPLES):
        self.samples = {}          # seq -> physical_value_uV
        self.last_seq = None
        self.maxlen = maxlen

    def add(self, seq, value, reason, quality_sink, patient_id, epoch_id, channel_id, event_time_ms):
        if seq in self.samples:
            quality_sink.append((patient_id, epoch_id, channel_id, event_time_ms, "duplicate", f"seq={seq}"))
            return
        if self.last_seq is not None and seq > self.last_seq + 1:
            quality_sink.append((patient_id, epoch_id, channel_id, event_time_ms, "gap",
                                  f"seq jumped {self.last_seq}->{seq}"))
        if self.last_seq is not None and seq < self.last_seq:
            quality_sink.append((patient_id, epoch_id, channel_id, event_time_ms, "out_of_order",
                                  f"seq {seq} arrived after {self.last_seq}"))
        self.samples[seq] = value
        if self.last_seq is None or seq > self.last_seq:
            self.last_seq = seq
        if reason in ("saturated", "broken", "line_noise"):
            quality_sink.append((patient_id, epoch_id, channel_id, event_time_ms, reason, f"seq={seq}"))
        if len(self.samples) > self.maxlen * 2:
            for k in sorted(self.samples)[: len(self.samples) - self.maxlen]:
                del self.samples[k]

    def window(self):
        keys = sorted(self.samples)[-self.maxlen:]
        return [self.samples[k] for k in keys]


def frame_label(channels_payload):
    """Most severe ground-truth state on any channel in the frame. Stored for
    detector evaluation only, never used as a formula input."""
    states = [ch.get("signal_state", "interictal") for ch in channels_payload.values()]
    return max(states, key=lambda s: _STATE_RANK.get(s, 0))


def process_frame(patient_id, epoch_id, frame_start_ms, channels_payload,
                   buffers, baselines, quality_events, duration_state):
    """Computes features and the surge index for one frame. Classification and
    episode tracking happen in the caller."""
    good_channels = set()
    bad_with_reason = {}
    ll_ratios = {}
    band_ratios = {}
    raw_ll = {}
    any_stim = False

    for cid, ch in channels_payload.items():
        buf = buffers.setdefault((patient_id, epoch_id, cid), ChannelBuffer())
        base_seq = ch.get("seq_start", 0)
        for i, v in enumerate(ch["physical_value_uV"]):
            buf.add(base_seq + i, v, ch["status_reason"],
                     quality_events, patient_id, epoch_id, cid, frame_start_ms)

        window = buf.window()
        ll = line_length(window)
        raw_ll[cid] = ll
        baseline = baselines[(patient_id, cid)]
        warm = len(baseline._buf) >= config.BASELINE_MIN_WINDOWS
        ll_ratios[cid] = baseline.ratio(ll) if warm else 1.0
        band_ratios[cid] = band_ratio(window)

        if ch["channel_status"] == "good":
            good_channels.add(cid)
        else:
            bad_with_reason[cid] = ch["status_reason"]

        stim_val = ch.get("stim_active")
        any_stim = any_stim or (any(stim_val) if isinstance(stim_val, list) else bool(stim_val))

    # learn only from clean windows: good channel, not elevated
    for cid, ll in raw_ll.items():
        baseline = baselines[(patient_id, cid)]
        warm = len(baseline._buf) >= config.BASELINE_MIN_WINDOWS
        if cid in good_channels and (not warm or ll_ratios[cid] < config.LL_RATIO_ELEVATED):
            baseline.update(ll)

    surge_eval = score_surge(ll_ratios, band_ratios, good_channels)

    # duration = consecutive frames at or above the warning surge index
    frame_ms = config.FRAME_TICKS * 1000.0 / config.FS
    prev = duration_state.get(patient_id, 0.0)
    duration_ms = prev + frame_ms if surge_eval.surge_index >= config.SI_WARNING else 0.0
    duration_state[patient_id] = duration_ms

    return surge_eval, bad_with_reason, any_stim, duration_ms


def insert_evaluation(cur, patient_id, epoch_id, packet_id, event_time_ms, surge_eval,
                       cls, fam, duration_ms, label_state, stim_active):
    cur.execute(
        """INSERT INTO surge_evaluations
           (patient_id, epoch_id, packet_id, event_time, surge_index, raw_amplitude,
            max_channel, n_elevated, m_spatial, m_soz, m_rhythm, duration_ms,
            severity, alert_type, seizure_severity, seizure_alert_type,
            quality_severity, device_severity, label_state, stim_active)
           VALUES (%s,%s,%s, FROM_UNIXTIME(%s/1000), %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
           ON DUPLICATE KEY UPDATE surge_index=VALUES(surge_index), severity=VALUES(severity),
               alert_type=VALUES(alert_type), seizure_severity=VALUES(seizure_severity),
               seizure_alert_type=VALUES(seizure_alert_type), quality_severity=VALUES(quality_severity),
               device_severity=VALUES(device_severity), label_state=VALUES(label_state),
               stim_active=VALUES(stim_active)""",
        (patient_id, epoch_id, packet_id, event_time_ms, surge_eval.surge_index,
         surge_eval.raw_amplitude, surge_eval.max_channel, surge_eval.n_elevated,
         surge_eval.m_spatial, surge_eval.m_soz, surge_eval.m_rhythm, duration_ms,
         cls.severity, cls.alert_type or "NONE",
         fam["seizure"][0], fam["seizure"][1] or "NONE",
         fam["quality"][0], fam["device"][0], label_state, stim_active),
    )


def flush_quality_events(cur, events):
    if not events:
        return
    cur.executemany(
        """INSERT IGNORE INTO data_quality_events
           (patient_id, epoch_id, channel_id, event_time, kind, detail)
           VALUES (%s,%s,%s, FROM_UNIXTIME(%s/1000), %s,%s)""",
        events,
    )
    events.clear()


def flush_episode(cur, ep):
    cur.execute(
        """INSERT INTO alert_episodes
           (patient_id, epoch_id, family, alert_type, severity, start_time, end_time, duration_ms,
            peak_surge_index, onset_channel, channels_involved, stim_delivered)
           VALUES (%s,%s,%s,%s,%s, FROM_UNIXTIME(%s/1000), FROM_UNIXTIME(%s/1000), %s,%s,%s,%s,%s)
           ON DUPLICATE KEY UPDATE end_time=VALUES(end_time), duration_ms=VALUES(duration_ms),
               alert_type=VALUES(alert_type),
               peak_surge_index=GREATEST(peak_surge_index, VALUES(peak_surge_index)),
               severity=VALUES(severity), stim_delivered=VALUES(stim_delivered)""",
        (ep.patient_id, ep.epoch_id, ep.family, ep.alert_type, ep.severity,
         ep.start_time_ms, ep.end_time_ms, ep.duration_ms(), ep.peak_surge_index,
         ep.onset_channel, ",".join(sorted(ep.channels_involved)), ep.stim_delivered),
    )


def handle_device_message(cur, d, device_states):
    ds = device_states[d["patient_id"]]
    ds.impedance_ohm[d["channel_id"]] = d["impedance_ohm"]
    charge = d["stim_current_mA"] * 1000.0 * config.PULSE_WIDTH_S / config.ELECTRODE_AREA_CM2
    ds.charge_density_uc_cm2[d["channel_id"]] = charge

    dev_sev, _ = classify_device(DeviceState(
        impedance_ohm={d["channel_id"]: d["impedance_ohm"]},
        charge_density_uc_cm2={d["channel_id"]: charge},
    ))
    cur.execute(
        """INSERT INTO device_health
           (patient_id, channel_id, event_time, impedance_ohm, stim_current_mA,
            charge_density_uc_cm2, severity)
           VALUES (%s,%s, FROM_UNIXTIME(%s/1000), %s,%s,%s,%s)
           ON DUPLICATE KEY UPDATE impedance_ohm=VALUES(impedance_ohm),
               stim_current_mA=VALUES(stim_current_mA),
               charge_density_uc_cm2=VALUES(charge_density_uc_cm2),
               severity=VALUES(severity)""",
        (d["patient_id"], d["channel_id"], d["event_ms"], d["impedance_ohm"],
         d["stim_current_mA"], charge, dev_sev),
    )


def merge_frame(pending, frame, all_lead_groups):
    """raw-voltage carries one lead group (2 channels) per Kafka message.
    The surge formula needs all 4 channels together (M_spatial specifically
    needs to see every good channel to tell a focal surge from a global
    artifact), so frames for the same (patient, epoch, packet_id) are joined
    here before scoring. Returns the merged frame once every lead group for
    that packet has arrived, else None.
    """
    key = (frame["patient_id"], frame["epoch_id"], frame["packet_id"])
    entry = pending.setdefault(key, {
        "patient_id": frame["patient_id"], "epoch_id": frame["epoch_id"],
        "packet_id": frame["packet_id"], "frame_start_ms": frame["frame_start_ms"],
        "channels": {}, "lead_groups": set(),
    })
    entry["channels"].update(frame["channels"])
    entry["lead_groups"].add(frame["lead_group"])
    entry["frame_start_ms"] = min(entry["frame_start_ms"], frame["frame_start_ms"])

    if entry["lead_groups"] >= all_lead_groups:
        del pending[key]
        return entry
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=config.BOOTSTRAP)
    ap.add_argument("--meta", default=META_CSV)
    args = ap.parse_args()

    meta = load_metadata(args.meta)
    all_lead_groups = {r["lead_group"] for r in meta.values()}

    consumer = KafkaConsumer(
        config.VOLTAGE_TOPIC, config.DEVICE_TOPIC,
        bootstrap_servers=args.bootstrap,
        group_id=config.CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset=config.AUTO_OFFSET_RESET,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )

    conn = get_connection()
    cur = conn.cursor()

    buffers = {}
    baselines = defaultdict(RobustBaseline)       # (patient_id, channel_id) -> baseline
    quality_events = []
    tracker = EpisodeTracker()
    device_states = defaultdict(DeviceState)     # patient_id -> DeviceState
    duration_state = {}
    pending_frames = {}                          # (patient,epoch,packet_id) -> partial merged frame

    print(f"listening on {config.VOLTAGE_TOPIC}, {config.DEVICE_TOPIC} ...")
    n_frames = 0
    for msg in consumer:
        if msg.topic == config.DEVICE_TOPIC:
            handle_device_message(cur, msg.value, device_states)
            conn.commit()
            consumer.commit()
            continue

        frame = merge_frame(pending_frames, msg.value, all_lead_groups)
        if frame is None:
            # only half the packet has arrived so far (other lead group still
            # in flight) -- nothing to score yet, just advance the offset
            consumer.commit()
            continue

        patient_id, epoch_id = frame["patient_id"], frame["epoch_id"]
        t_ms = frame["frame_start_ms"]
        surge_eval, bad_with_reason, any_stim, duration_ms = process_frame(
            patient_id, epoch_id, t_ms, frame["channels"],
            buffers, baselines, quality_events, duration_state,
        )
        ds = device_states[patient_id]
        cls = classify(surge_eval, duration_ms, bad_with_reason, ds,
                        active_seizure=surge_eval.surge_index >= config.SI_WARNING)
        fam = classify_families(surge_eval, duration_ms, bad_with_reason, ds)

        sz_sev, sz_type = fam["seizure"]
        q_sev, q_type = fam["quality"]
        open_eps = [
            tracker.update(patient_id, epoch_id, "SEIZURE", sz_type, sz_sev, t_ms,
                            surge_eval.surge_index, surge_eval.max_channel, any_stim),
            tracker.update(patient_id, epoch_id, "SIGNAL_QUALITY", q_type, q_sev, t_ms,
                            0.0, next(iter(bad_with_reason), None), False),
        ]

        insert_evaluation(cur, patient_id, epoch_id, frame["packet_id"], t_ms,
                           surge_eval, cls, fam, duration_ms, frame_label(frame["channels"]), any_stim)
        flush_quality_events(cur, quality_events)
        # upsert in-progress episodes too, so the on-call view sees them live
        for ep in [e for e in open_eps if e is not None] + tracker.drain_closed():
            flush_episode(cur, ep)
        conn.commit()
        consumer.commit()

        n_frames += 1
        if sz_sev == "critical":
            print(f"[CRITICAL] {patient_id} {surge_eval.max_channel} "
                  f"SI={surge_eval.surge_index:.2f} D={duration_ms:.0f}ms")
        if n_frames % 1000 == 0:
            print(f"processed {n_frames} frames")


if __name__ == "__main__":
    main()
