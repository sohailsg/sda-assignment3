-- =====================================================================
-- Dashboard queries: every SQL statement backing every panel in
-- dashboard/dashA_realtime_clinical.json and dashboard/dashB_longitudinal_review.json.
--
-- These are pasted verbatim from the Grafana panel JSON, so they contain
-- Grafana's own macros and template variables. Legend to read/run them:
--
--   $__timeFilter(col)        ->  col BETWEEN <range start> AND <range end>
--                                  (the dashboard's selected time range)
--   $__timeGroupAlias(col,'7d') -> buckets col into 7-day windows, aliased "time"
--                                  (use any interval: '1s', '10s', '1h', '7d', ...)
--   $patient / $focus_patient -> a single patient_id, or a comma-separated
--                                  list when the variable is multi-select
--                                  (e.g. patient_id IN ('PT-0042','PT-0117'))
--
-- To run one of these directly in a MySQL client, replace the macros with
-- literal values, e.g.:
--   $__timeFilter(event_time)   -> event_time BETWEEN '2026-09-01' AND '2026-09-30'
--   patient_id IN ($patient)    -> patient_id IN ('PT-0042')
-- =====================================================================


-- =====================================================================
-- DASHBOARD A: Real-Time Clinical Dashboard
-- =====================================================================

-- Panel: Active Seizures (KPI stat)
-- One row per discrete alert episode -- NOT per 32ms frame, otherwise a
-- single seizure (which can span 20+ consecutive critical frames) would
-- inflate this count by 20x.
SELECT COUNT(*) AS value
FROM alert_episodes
WHERE severity = 'critical'
  AND $__timeFilter(start_time)
  AND patient_id IN ($patient);

-- Panel: Possible / Mild Events (KPI stat)
SELECT COUNT(*) AS value
FROM alert_episodes
WHERE severity = 'warning'
  AND $__timeFilter(start_time)
  AND patient_id IN ($patient);

-- Panel: Patients Monitored (KPI stat)
SELECT COUNT(DISTINCT patient_id) AS value
FROM surge_evaluations
WHERE patient_id IN ($patient);

-- Panel: Device / Sensor Faults (KPI stat)
-- Separate track from the alert episodes above: device faults are continuous
-- hardware telemetry (impedance / charge density), not discrete seizure events.
SELECT COUNT(*) AS value
FROM device_health
WHERE severity != 'normal'
  AND $__timeFilter(event_time)
  AND patient_id IN ($patient);

-- Panel: Active Alerts (table)
-- One row per episode. "Spread" derives focality from how many channels were
-- involved (<=2 = focal seizure shape, 3+ = looks like a whole-head artifact).
SELECT
    e.start_time                                            AS 'Time',
    e.patient_id                                             AS 'Patient',
    e.severity                                                AS 'Severity',
    CASE e.alert_type
        WHEN 'SEIZURE_SURGE'   THEN 'Seizure'
        WHEN 'SUBCLINICAL'     THEN 'Mild / brief event'
        WHEN 'SIGNAL_QUALITY'  THEN 'Bad sensor data'
        ELSE e.alert_type
    END                                                        AS 'What Happened',
    c.anatomical_location                                     AS 'Brain Region',
    CASE
        WHEN (LENGTH(e.channels_involved)
              - LENGTH(REPLACE(e.channels_involved, ',', '')) + 1) <= 2
        THEN 'Focal' ELSE 'Widespread'
    END                                                        AS 'Spread',
    ROUND(e.duration_ms)                                       AS 'Duration (ms)',
    e.stim_delivered                                           AS 'Treatment Fired'
FROM alert_episodes e
LEFT JOIN channels c ON c.channel_id = e.onset_channel
WHERE $__timeFilter(e.start_time)
  AND e.patient_id IN ($patient)
ORDER BY e.end_time DESC
LIMIT 30;

-- Panel: Seizure Intensity & Treatment Response (timeseries, drill-down to one patient)
-- Query A: the Surge Index itself, 1-second resolution.
SELECT $__timeGroupAlias(event_time, '1s'),
       'intensity' AS metric,
       MAX(surge_index) AS value
FROM surge_evaluations
WHERE $__timeFilter(event_time)
  AND patient_id = '$focus_patient'
GROUP BY 1, 2
ORDER BY 1;

-- Query B: marks the exact moments the implant's stimulation actually fired,
-- overlaid on the same chart as points.
SELECT event_time AS time,
       'treatment_fired' AS metric,
       surge_index AS value
FROM surge_evaluations
WHERE $__timeFilter(event_time)
  AND patient_id = '$focus_patient'
  AND stim_active = 1
ORDER BY event_time;


-- =====================================================================
-- DASHBOARD B: Longitudinal Clinical Review
-- =====================================================================

-- Panel: Seizures — Last 14 Days (KPI stat)
SELECT COUNT(*) AS value
FROM alert_episodes
WHERE family = 'SEIZURE'
  AND patient_id = '$patient'
  AND start_time > NOW() - INTERVAL 14 DAY;

-- Panel: Weekly Trend (KPI stat)
-- A linear-regression slope (seizures/week, change per week) fitted across
-- every week of available data. Deliberately NOT a two-point
-- "this period vs last period" percentage: with only ~10 episodes per
-- 14-day bucket, a two-point comparison is dominated by sampling noise
-- (two different patients can land on the identical percentage by chance).
-- The slope formula: slope = (n*Sum(w*c) - Sum(w)*Sum(c)) / (n*Sum(w^2) - Sum(w)^2)
-- where w = week index (0,1,2,...) and c = episode count in that week.
SELECT (COUNT(*) * SUM(w * c) - SUM(w) * SUM(c))
       / NULLIF(COUNT(*) * SUM(w * w) - POWER(SUM(w), 2), 0) AS value
FROM (
    SELECT FLOOR(DATEDIFF(
               start_time,
               (SELECT MIN(start_time) FROM alert_episodes
                WHERE family = 'SEIZURE' AND patient_id = '$patient')
           ) / 7) AS w,
           COUNT(*) AS c
    FROM alert_episodes
    WHERE family = 'SEIZURE'
      AND $__timeFilter(start_time)
      AND patient_id = '$patient'
    GROUP BY w
) t;

-- Panel: Treatment Response Rate (KPI stat)
-- "Did stimulation fire anywhere during this seizure's recording" rather than
-- "did stimulation overlap the exact alert-episode window" -- the implant's
-- therapy fires at seizure onset, but the statistical detector's sliding
-- window takes ~250ms to ramp up and declare an alert, so a strict window
-- match would always read 0% even when the device responded correctly.
SELECT 100.0 * SUM(
                  CASE WHEN EXISTS (
                      SELECT 1 FROM surge_evaluations s
                      WHERE s.patient_id = e.patient_id
                        AND s.epoch_id = e.epoch_id
                        AND s.stim_active = 1
                  ) THEN 1 ELSE 0 END
              ) / NULLIF(COUNT(*), 0) AS value
FROM alert_episodes e
WHERE e.family = 'SEIZURE'
  AND e.severity = 'critical'
  AND $__timeFilter(e.start_time)
  AND e.patient_id = '$patient';

-- Panel: Seizures per Week (stacked bar, by severity)
SELECT $__timeGroupAlias(start_time, '7d'),
       severity AS metric,
       COUNT(*) AS value
FROM alert_episodes
WHERE family = 'SEIZURE'
  AND $__timeFilter(start_time)
  AND patient_id = '$patient'
GROUP BY 1, 2
ORDER BY 1;

-- Panel: Average Seizure Length per Week (timeseries)
SELECT $__timeGroupAlias(start_time, '7d'),
       patient_id AS metric,
       AVG(duration_ms) AS value
FROM alert_episodes
WHERE family = 'SEIZURE'
  AND $__timeFilter(start_time)
  AND patient_id = '$patient'
GROUP BY 1, 2
ORDER BY 1;

-- Panel: Average Seizure Intensity per Week (timeseries)
-- Kept as its own chart rather than a second y-axis on the duration chart
-- above, since the two are different units/scales (a dual-axis chart makes
-- the reader infer a correlation that isn't really there).
SELECT $__timeGroupAlias(start_time, '7d'),
       patient_id AS metric,
       AVG(peak_surge_index) AS value
FROM alert_episodes
WHERE family = 'SEIZURE'
  AND $__timeFilter(start_time)
  AND patient_id = '$patient'
GROUP BY 1, 2
ORDER BY 1;

-- Panel: Where This Patient's Seizures Start (bar chart)
SELECT c.anatomical_location AS onset,
       COUNT(*) AS n_episodes
FROM alert_episodes e
JOIN channels c ON c.channel_id = e.onset_channel
WHERE e.family = 'SEIZURE'
  AND $__timeFilter(e.start_time)
  AND e.patient_id = '$patient'
GROUP BY onset
ORDER BY n_episodes DESC;
