"""Alert scoring/classification formula. Pure functions, no I/O.

See docs/IMPLEMENTATION_GUIDE.md Phase 3 for the full research rationale:
max() across channels (not weighted average) so a single focal channel isn't
diluted, multipliers to add context (spatial spread, SOZ, rhythmicity), and
categorical severity bands per rule family with the worst family winning.
"""

from dataclasses import dataclass, field

from pipeline.config import (
    BAND_RATIO_RHYTHMIC,
    CHARGE_DENSITY_CRITICAL_UC_CM2,
    CRITICAL_DURATION_MS,
    IMPEDANCE_MAX_OHM,
    IMPEDANCE_MIN_OHM,
    LL_RATIO_ELEVATED,
    M_RHYTHM_LOW,
    M_SOZ_BONUS,
    M_SPATIAL_FOCAL,
    M_SPATIAL_GLOBAL,
    M_SPATIAL_MIXED,
    SI_CRITICAL,
    SI_WARNING,
    SOZ_CHANNELS,
)

NORMAL, WARNING, CRITICAL = "normal", "warning", "critical"
_SEVERITY_RANK = {NORMAL: 0, WARNING: 1, CRITICAL: 2}

SEIZURE_SURGE = "SEIZURE_SURGE"
SUBCLINICAL = "SUBCLINICAL"
ARTIFACT = "ARTIFACT"
SIGNAL_QUALITY = "SIGNAL_QUALITY"
DEVICE_SAFETY = "DEVICE_SAFETY"


@dataclass
class SurgeEval:
    """Result of the spatial/spectral surge-index formula for one frame,
    evaluated across all good channels of a patient."""

    surge_index: float
    raw_amplitude: float          # A = max_c ll_ratio_c, before multipliers
    max_channel: str
    n_elevated: int
    n_good: int
    m_spatial: float
    m_soz: float
    m_rhythm: float
    band_ratio_max: float
    is_artifact_shaped: bool      # raw amplitude high but spatially global


def score_surge(ll_ratios, band_ratios, good_channels):
    """Compute the surge index for one frame.

    ll_ratios: {channel_id: line_length_ratio}
    band_ratios: {channel_id: band_ratio} (13-30Hz fraction)
    good_channels: set of channel_ids currently status=='good'
    """
    good_ratios = {c: r for c, r in ll_ratios.items() if c in good_channels}
    if not good_ratios:
        return SurgeEval(0.0, 0.0, None, 0, 0, 1.0, 1.0, 1.0, 0.0, False)

    max_channel = max(good_ratios, key=good_ratios.get)
    amplitude = good_ratios[max_channel]
    n_good = len(good_ratios)
    n_elevated = sum(1 for r in good_ratios.values() if r >= LL_RATIO_ELEVATED)

    if n_elevated <= 2:
        m_spatial = M_SPATIAL_FOCAL
    elif n_elevated >= 3 and n_elevated == n_good:
        m_spatial = M_SPATIAL_GLOBAL
    else:
        m_spatial = M_SPATIAL_MIXED

    m_soz = M_SOZ_BONUS if max_channel in SOZ_CHANNELS else 1.0

    band_ratio_max = band_ratios.get(max_channel, 0.0)
    m_rhythm = 1.0 if band_ratio_max >= BAND_RATIO_RHYTHMIC else M_RHYTHM_LOW

    surge_index = amplitude * m_spatial * m_soz * m_rhythm
    is_artifact_shaped = amplitude >= SI_CRITICAL and m_spatial <= M_SPATIAL_GLOBAL

    return SurgeEval(
        surge_index=surge_index,
        raw_amplitude=amplitude,
        max_channel=max_channel,
        n_elevated=n_elevated,
        n_good=n_good,
        m_spatial=m_spatial,
        m_soz=m_soz,
        m_rhythm=m_rhythm,
        band_ratio_max=band_ratio_max,
        is_artifact_shaped=is_artifact_shaped,
    )


@dataclass
class DeviceState:
    impedance_ohm: dict = field(default_factory=dict)   # channel_id -> ohm
    charge_density_uc_cm2: dict = field(default_factory=dict)  # channel_id -> uC/cm^2


def classify_device(device_state: DeviceState):
    """Device-safety family: worst channel wins, hard-limit categorical."""
    severity = NORMAL
    reasons = []
    for cid, z in device_state.impedance_ohm.items():
        if z < IMPEDANCE_MIN_OHM or z > IMPEDANCE_MAX_OHM:
            severity = max(severity, WARNING, key=_SEVERITY_RANK.get)
            reasons.append(f"{cid} impedance {z:.0f}ohm out of [{IMPEDANCE_MIN_OHM},{IMPEDANCE_MAX_OHM}]")
    for cid, q in device_state.charge_density_uc_cm2.items():
        if q > CHARGE_DENSITY_CRITICAL_UC_CM2:
            severity = CRITICAL
            reasons.append(f"{cid} charge density {q:.1f}uC/cm2 > {CHARGE_DENSITY_CRITICAL_UC_CM2}")
    return severity, reasons


def classify_signal_quality(bad_channels_with_reason, active_seizure):
    """Signal-quality family. Saturation during an active seizure on a SOZ
    channel is escalated to CRITICAL (blind during the event that matters
    most); any other bad channel is a WARNING."""
    severity = NORMAL
    reasons = []
    for cid, reason in bad_channels_with_reason.items():
        reasons.append(f"{cid}:{reason}")
        if reason == "saturated" and cid in SOZ_CHANNELS and active_seizure:
            severity = CRITICAL
        else:
            severity = max(severity, WARNING, key=_SEVERITY_RANK.get)
    return severity, reasons


def classify_seizure(surge_eval: SurgeEval, duration_ms: float):
    """Seizure family from the surge index + how long it has persisted."""
    si = surge_eval.surge_index
    if surge_eval.is_artifact_shaped:
        return NORMAL, ARTIFACT, [
            f"amplitude={surge_eval.raw_amplitude:.2f} but spatially global "
            f"(n_elevated={surge_eval.n_elevated}/{surge_eval.n_good}) -> treated as artifact"
        ]
    if si >= SI_CRITICAL and duration_ms >= CRITICAL_DURATION_MS:
        return CRITICAL, SEIZURE_SURGE, [f"SI={si:.2f} for {duration_ms:.0f}ms on {surge_eval.max_channel}"]
    if si >= SI_WARNING:
        alert_type = SEIZURE_SURGE if si >= SI_CRITICAL else SUBCLINICAL
        return WARNING, alert_type, [f"SI={si:.2f} for {duration_ms:.0f}ms on {surge_eval.max_channel}"]
    return NORMAL, None, []


@dataclass
class Classification:
    severity: str
    alert_type: str
    reasons: list


def classify(surge_eval: SurgeEval, duration_ms, bad_channels_with_reason,
             device_state: DeviceState, active_seizure=False):
    """Top-level classification: max() across independent rule families,
    never a weighted average (see Phase 3.1 for the rationale)."""
    families = []

    sz_sev, sz_type, sz_reasons = classify_seizure(surge_eval, duration_ms)
    families.append((sz_sev, sz_type, sz_reasons))

    sq_sev, sq_reasons = classify_signal_quality(bad_channels_with_reason, active_seizure)
    if sq_sev != NORMAL:
        families.append((sq_sev, SIGNAL_QUALITY, sq_reasons))

    dv_sev, dv_reasons = classify_device(device_state)
    if dv_sev != NORMAL:
        families.append((dv_sev, DEVICE_SAFETY, dv_reasons))

    # tie-break priority when severities are equal: Seizure > Device > Quality
    priority = {SEIZURE_SURGE: 0, SUBCLINICAL: 0, ARTIFACT: 3, DEVICE_SAFETY: 1, SIGNAL_QUALITY: 2, None: 4}
    families.sort(key=lambda f: (-_SEVERITY_RANK[f[0]], priority.get(f[1], 9)))

    winner = families[0]
    all_reasons = [r for _, _, rs in families for r in rs]
    return Classification(severity=winner[0], alert_type=winner[1], reasons=all_reasons)


def classify_families(surge_eval: SurgeEval, duration_ms, bad_channels_with_reason,
                      device_state: DeviceState):
    """Per-family results, kept separate for storage.

    A persistent device condition (e.g. one electrode with high impedance) is a
    maintenance issue, not an event. If it joined the frame-level max() it would
    stamp every voltage frame and hide seizures, so each family is stored in its
    own column and gets its own episodes. `classify()` still gives the combined
    worst-case severity.
    """
    sz_sev, sz_type, _ = classify_seizure(surge_eval, duration_ms)
    active = surge_eval.surge_index >= SI_WARNING
    sq_sev, _ = classify_signal_quality(bad_channels_with_reason, active)
    dv_sev, _ = classify_device(device_state)
    return {
        "seizure": (sz_sev, sz_type),
        "quality": (sq_sev, SIGNAL_QUALITY if sq_sev != NORMAL else None),
        "device": (dv_sev, DEVICE_SAFETY if dv_sev != NORMAL else None),
    }
