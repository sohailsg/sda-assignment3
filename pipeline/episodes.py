"""Episode state machine: turns a per-frame severity stream into discrete
alert episodes (one row per seizure / signal-quality event) for clinical
review. See docs/IMPLEMENTATION_GUIDE.md Phase 5-6.

Episodes are keyed by (patient_id, family), not by alert_type: a seizure
ramps through SUBCLINICAL before reaching SEIZURE_SURGE, and it is still one
seizure. The episode's alert_type is upgraded as it escalates.
"""

from dataclasses import dataclass, field

from pipeline.alert_rules import CRITICAL, NORMAL, SEIZURE_SURGE, SUBCLINICAL
from pipeline.config import EPISODE_CLOSE_NORMAL_FRAMES

# within a family, a later type only replaces an earlier one if it ranks higher
_TYPE_RANK = {SUBCLINICAL: 0, SEIZURE_SURGE: 1}


@dataclass
class Episode:
    patient_id: str
    epoch_id: str
    family: str
    alert_type: str
    severity: str
    start_time_ms: int
    end_time_ms: int
    peak_surge_index: float = 0.0
    onset_channel: str = None
    channels_involved: set = field(default_factory=set)
    stim_delivered: bool = False
    normal_streak: int = 0

    def duration_ms(self):
        return self.end_time_ms - self.start_time_ms


class EpisodeTracker:
    """Call update() once per frame per family."""

    def __init__(self):
        self._open = {}   # (patient_id, family) -> Episode
        self._closed = []

    def update(self, patient_id, epoch_id, family, alert_type, severity, event_time_ms,
               surge_index, channel, stim_active):
        key = (patient_id, family)
        ep = self._open.get(key)

        # a new recording epoch always ends the previous episode
        if ep is not None and ep.epoch_id != epoch_id:
            self._closed.append(self._open.pop(key))
            ep = None

        if severity == NORMAL or alert_type is None:
            if ep is not None:
                ep.normal_streak += 1
                if ep.normal_streak >= EPISODE_CLOSE_NORMAL_FRAMES:
                    self._closed.append(self._open.pop(key))
            return None

        if ep is None:
            ep = Episode(
                patient_id=patient_id, epoch_id=epoch_id, family=family,
                alert_type=alert_type, severity=severity,
                start_time_ms=event_time_ms, end_time_ms=event_time_ms,
                onset_channel=channel,
            )
            self._open[key] = ep

        ep.end_time_ms = event_time_ms
        ep.normal_streak = 0
        ep.peak_surge_index = max(ep.peak_surge_index, surge_index)
        if _TYPE_RANK.get(alert_type, 0) > _TYPE_RANK.get(ep.alert_type, 0):
            ep.alert_type = alert_type
        if channel:
            ep.channels_involved.add(channel)
        if stim_active:
            ep.stim_delivered = True
        # severity only escalates within an episode, never downgrades it
        if severity == CRITICAL:
            ep.severity = CRITICAL
        return ep

    def close_all(self):
        self._closed.extend(self._open.values())
        self._open.clear()

    def drain_closed(self):
        closed, self._closed = self._closed, []
        return closed
