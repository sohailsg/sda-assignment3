"""Pure signal-feature functions. No I/O, no Kafka/MySQL — unit-testable in
isolation. See docs/IMPLEMENTATION_GUIDE.md Phase 1.3 for feature rationale.
"""

from collections import deque

import numpy as np

from pipeline.config import BASELINE_WARMUP_WINDOWS, FS


def line_length(x):
    """Sum of absolute successive differences — amplitude x frequency in one
    number. RNS's own primary detection statistic."""
    x = np.asarray(x, dtype=float)
    if len(x) < 2:
        return 0.0
    return float(np.sum(np.abs(np.diff(x))))


def band_ratio(x, fs=FS, lo=13.0, hi=30.0, full_lo=4.0, full_hi=90.0):
    """Fraction of spectral power in [lo, hi] Hz vs the full sensing band.
    Orthogonal to amplitude -- separates rhythmic seizure activity (13-30Hz)
    from broadband artifact (EMG/movement)."""
    x = np.asarray(x, dtype=float)
    n = len(x)
    if n < 4:
        return 0.0
    windowed = x * np.hanning(n)
    spectrum = np.abs(np.fft.rfft(windowed)) ** 2
    freqs = np.fft.rfftfreq(n, d=1.0 / fs)

    full_mask = (freqs >= full_lo) & (freqs <= full_hi)
    band_mask = (freqs >= lo) & (freqs <= hi)

    full_power = spectrum[full_mask].sum()
    if full_power <= 0:
        return 0.0
    band_power = spectrum[band_mask].sum()
    return float(band_power / full_power)


class RobustBaseline:
    """Per-channel rolling median of line-length over "normal" windows.
    Used as the denominator for ll_ratio. Median (not mean) so a single
    artifact window during warm-up doesn't skew the baseline.
    """

    def __init__(self, maxlen=BASELINE_WARMUP_WINDOWS, min_value=1e-6):
        self._buf = deque(maxlen=maxlen)
        self._min_value = min_value

    def update(self, ll_value):
        self._buf.append(ll_value)

    @property
    def ready(self):
        return len(self._buf) >= 1

    @property
    def value(self):
        if not self._buf:
            return self._min_value
        return max(float(np.median(self._buf)), self._min_value)

    def ratio(self, ll_value):
        return ll_value / self.value


def charge_density_uc_cm2(current_mA, pulse_width_s, area_cm2):
    """Charge density per phase, uC/cm^2. Deterministic from stim current --
    never sent over the wire as its own feature (Phase 1.3: drop derived
    quantities)."""
    # current_mA * pulse_width_s = charge in mC; x1000 -> uC.
    charge_uc = current_mA * 1000.0 * pulse_width_s
    return charge_uc / area_cm2
