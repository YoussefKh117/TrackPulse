-- TrackPulse: Real-Time Alerts and Five Window Analytics

WITH Telemetry AS (
    SELECT *
    FROM [telemetryinput] TIMESTAMP BY event_timestamp
)

-- 1. Real-Time Alerts
SELECT
    event_id,
    event_timestamp,
    vehicle_id,
    engine_temp_c,
    cargo_temp_c,
    speed_kph,
    ignition_on,
    CASE
        WHEN engine_temp_c > 105 THEN 'ENGINE_OVERHEAT'
        WHEN cargo_temp_c < -1 OR cargo_temp_c > 2 THEN 'CARGO_TEMP_BREACH'
        ELSE 'IDLING_CANDIDATE'
    END AS alert_type,
    CASE
        WHEN engine_temp_c > 105 THEN 'HIGH'
        WHEN cargo_temp_c < -1 OR cargo_temp_c > 2 THEN 'HIGH'
        ELSE 'MEDIUM'
    END AS severity,
    test_mode,
    test_id,
    System.Timestamp() AS processed_at
INTO [alertsoutput]
FROM Telemetry
WHERE
    engine_temp_c > 105
    OR cargo_temp_c < -1
    OR cargo_temp_c > 2
    OR (ignition_on = 1 AND speed_kph <= 2)

-- 2. Engine Temperature: 30s
SELECT
    System.Timestamp() AS window_end,
    vehicle_id,
    COUNT(*) AS event_count,
    AVG(engine_temp_c) AS avg_engine_temp_c,
    MAX(engine_temp_c) AS max_engine_temp_c,
    MIN(engine_temp_c) AS min_engine_temp_c,
    SUM(CASE WHEN engine_temp_c > 105 THEN 1 ELSE 0 END) AS overheat_event_count
INTO [enginewindowoutput]
FROM Telemetry
WHERE
    vehicle_id IS NOT NULL
    AND (test_mode IS NULL OR test_mode = 0)
GROUP BY
    vehicle_id,
    TumblingWindow(second, 30)

-- 3. Cold Chain: 30s
SELECT
    System.Timestamp() AS window_end,
    vehicle_id,
    COUNT(*) AS event_count,
    AVG(cargo_temp_c) AS avg_cargo_temp_c,
    MIN(cargo_temp_c) AS min_cargo_temp_c,
    MAX(cargo_temp_c) AS max_cargo_temp_c,
    SUM(CASE WHEN cargo_temp_c < -1 OR cargo_temp_c > 2 THEN 1 ELSE 0 END) AS cargo_breach_event_count
INTO [coldchainwindowoutput]
FROM Telemetry
WHERE
    vehicle_id IS NOT NULL
    AND cargo_temp_c IS NOT NULL
    AND (test_mode IS NULL OR test_mode = 0)
GROUP BY
    vehicle_id,
    TumblingWindow(second, 30)

-- 4. Vehicle Speed: 60s
SELECT
    System.Timestamp() AS window_end,
    vehicle_id,
    COUNT(*) AS event_count,
    AVG(speed_kph) AS avg_speed_kph,
    MIN(speed_kph) AS min_speed_kph,
    MAX(speed_kph) AS max_speed_kph
INTO [speedwindowoutput]
FROM Telemetry
WHERE
    vehicle_id IS NOT NULL
    AND speed_kph IS NOT NULL
    AND (test_mode IS NULL OR test_mode = 0)
GROUP BY
    vehicle_id,
    TumblingWindow(second, 60)

-- 5. Idling Analysis: 60s
SELECT
    System.Timestamp() AS window_end,
    vehicle_id,
    COUNT(*) AS event_count,
    SUM(CASE WHEN ignition_on = 1 AND speed_kph <= 2 THEN 1 ELSE 0 END) AS idling_candidate_count,
    AVG(speed_kph) AS avg_speed_kph
INTO [idlingwindowoutput]
FROM Telemetry
WHERE
    vehicle_id IS NOT NULL
    AND (test_mode IS NULL OR test_mode = 0)
GROUP BY
    vehicle_id,
    TumblingWindow(second, 60)

-- 6. General Telemetry: 60s
SELECT
    System.Timestamp() AS window_end,
    vehicle_id,
    COUNT(*) AS event_count,
    AVG(CAST(rpm AS float)) AS avg_rpm,
    MAX(rpm) AS max_rpm,
    AVG(fuel_rate_lph) AS avg_fuel_rate_lph,
    AVG(speed_kph) AS avg_speed_kph
INTO [telemetrywindowoutput]
FROM Telemetry
WHERE
    vehicle_id IS NOT NULL
    AND (test_mode IS NULL OR test_mode = 0)
GROUP BY
    vehicle_id,
    TumblingWindow(second, 60)