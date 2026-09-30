-- Surge-monitoring schema, added into the existing `stockdb` database
-- (docker-compose.yml is left untouched, so we reuse its DB rather than
-- renaming it). Safe to re-run: every CREATE is IF NOT EXISTS.
-- To reset demo data: DROP the four fact tables, then re-run this file.
-- See docs/IMPLEMENTATION_GUIDE.md Phase 5-6 for design rationale.

CREATE TABLE IF NOT EXISTS surge_evaluations (
    patient_id      VARCHAR(16)  NOT NULL,
    epoch_id        VARCHAR(24)  NOT NULL,
    packet_id       INT          NOT NULL,
    event_time      DATETIME(3)  NOT NULL,
    processed_at    TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
    surge_index     DOUBLE       NOT NULL,
    raw_amplitude   DOUBLE       NOT NULL,
    max_channel     VARCHAR(8),
    n_elevated      INT,
    m_spatial       DOUBLE,
    m_soz           DOUBLE,
    m_rhythm        DOUBLE,
    duration_ms     DOUBLE,
    severity        ENUM('normal','warning','critical') NOT NULL,   -- worst of all families
    alert_type      VARCHAR(32) NOT NULL,
    seizure_severity   ENUM('normal','warning','critical') NOT NULL,
    seizure_alert_type VARCHAR(32) NOT NULL,     -- SEIZURE_SURGE / SUBCLINICAL / ARTIFACT / NONE
    quality_severity   ENUM('normal','warning','critical') NOT NULL,
    device_severity    ENUM('normal','warning','critical') NOT NULL,
    label_state     VARCHAR(16),                 -- synthetic ground truth, evaluation only
    PRIMARY KEY (patient_id, epoch_id, packet_id),
    INDEX idx_surge_eval_time (patient_id, event_time)
);

CREATE TABLE IF NOT EXISTS alert_episodes (
    episode_id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    patient_id           VARCHAR(16) NOT NULL,
    epoch_id             VARCHAR(24) NOT NULL,
    family               VARCHAR(16) NOT NULL,   -- SEIZURE / SIGNAL_QUALITY
    alert_type           VARCHAR(32) NOT NULL,
    severity             ENUM('normal','warning','critical') NOT NULL,
    start_time            DATETIME(3) NOT NULL,
    end_time              DATETIME(3) NOT NULL,
    duration_ms           DOUBLE,
    peak_surge_index       DOUBLE,
    onset_channel          VARCHAR(8),
    channels_involved      VARCHAR(64),
    stim_delivered          BOOLEAN DEFAULT FALSE,
    UNIQUE KEY uq_episode (patient_id, epoch_id, family, start_time),
    INDEX idx_episode_time (patient_id, start_time)
);

CREATE TABLE IF NOT EXISTS device_health (
    patient_id            VARCHAR(16) NOT NULL,
    channel_id            VARCHAR(8) NOT NULL,
    event_time            DATETIME(3) NOT NULL,
    impedance_ohm         DOUBLE,
    stim_current_mA        DOUBLE,
    charge_density_uc_cm2   DOUBLE,
    severity               ENUM('normal','warning','critical') DEFAULT 'normal',
    PRIMARY KEY (patient_id, channel_id, event_time)
);

CREATE TABLE IF NOT EXISTS data_quality_events (
    id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    patient_id     VARCHAR(16) NOT NULL,
    epoch_id       VARCHAR(24) NOT NULL,
    channel_id     VARCHAR(8) NOT NULL,
    event_time     DATETIME(3) NOT NULL,
    kind           ENUM('gap','duplicate','out_of_order','saturated','broken','line_noise') NOT NULL,
    detail         VARCHAR(255),
    UNIQUE KEY uq_quality_event (patient_id, epoch_id, channel_id, event_time, kind),
    INDEX idx_quality_time (patient_id, event_time)
);

CREATE TABLE IF NOT EXISTS channels (
    channel_id             VARCHAR(8) PRIMARY KEY,
    channel_name           VARCHAR(32),
    lead_group             VARCHAR(16),
    hemisphere             VARCHAR(4),
    anatomical_location    VARCHAR(64),
    is_seizure_onset_zone  BOOLEAN,
    impedance_ohm          DOUBLE,
    impedance_status       VARCHAR(16)
);

-- Reporting views (Phase 5: reads should not need to know table internals)
CREATE OR REPLACE VIEW v_episode_summary AS
SELECT patient_id, alert_type, severity,
       COUNT(*) AS n_episodes,
       AVG(duration_ms) AS avg_duration_ms,
       AVG(peak_surge_index) AS avg_peak_si,
       SUM(stim_delivered) AS n_with_stim
FROM alert_episodes
GROUP BY patient_id, alert_type, severity;

CREATE OR REPLACE VIEW v_channel_quality AS
SELECT channel_id, kind, COUNT(*) AS n_events
FROM data_quality_events
GROUP BY channel_id, kind;

-- One row per recording epoch: did it truly contain a seizure (ground truth),
-- did the detector raise a CRITICAL seizure alert, and how late was it.
-- Answers "is the detector catching real seizures, and at what false-alarm cost?"
CREATE OR REPLACE VIEW v_epoch_detection AS
SELECT patient_id,
       epoch_id,
       MIN(event_time)                                                       AS epoch_start,
       MAX(label_state = 'ictal')                                            AS has_seizure,
       MAX(seizure_severity = 'critical')                                    AS detected_critical,
       MAX(seizure_severity IN ('warning','critical'))                       AS detected_any,
       MIN(CASE WHEN label_state = 'ictal' THEN event_time END)              AS ictal_onset,
       MIN(CASE WHEN seizure_severity = 'critical' THEN event_time END)      AS first_critical,
       TIMESTAMPDIFF(MICROSECOND,
           MIN(CASE WHEN label_state = 'ictal' THEN event_time END),
           MIN(CASE WHEN seizure_severity = 'critical' THEN event_time END)) / 1000 AS detection_delay_ms,
       SUM(seizure_alert_type = 'ARTIFACT')                                  AS artifact_frames
FROM surge_evaluations
GROUP BY patient_id, epoch_id;

-- Per-frame stim flag, needed for "did therapy fire" overlay on the live SI chart.
-- (MySQL 8.0 on this image doesn't support ADD COLUMN IF NOT EXISTS, so guard manually.)
SET @col_exists = (SELECT COUNT(*) FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'surge_evaluations'
                      AND COLUMN_NAME = 'stim_active');
SET @sql = IF(@col_exists = 0,
    'ALTER TABLE surge_evaluations ADD COLUMN stim_active BOOLEAN DEFAULT FALSE',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
