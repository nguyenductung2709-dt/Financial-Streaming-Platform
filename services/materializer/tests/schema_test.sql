-- Run against a database where migration 001 has been applied.
-- All fixtures are rolled back, including when psql exits on a test failure.
BEGIN;

INSERT INTO symbols VALUES ('__SCHEMA_TEST__.FR') ON CONFLICT DO NOTHING;
INSERT INTO symbols VALUES ('__SCHEMA_TEST__.FR') ON CONFLICT DO NOTHING;

INSERT INTO ema_windows VALUES (
    '__SCHEMA_TEST__.FR', '2021-11-12 06:00:00+00', '2021-11-12 06:05:00+00',
    18446744073709551615,
    1.234567890123456789012345678901234,
    0.000000000000000000000000000000001
);

-- Replayed results use the same business key, without adding another row.
INSERT INTO ema_windows
SELECT * FROM ema_windows WHERE symbol = '__SCHEMA_TEST__.FR'
ON CONFLICT (symbol, window_start) DO UPDATE
SET ema38 = EXCLUDED.ema38, ema100 = EXCLUDED.ema100;

INSERT INTO trade_advisories VALUES (
    '__SCHEMA_TEST__.FR', '2021-11-12 08:00:00+02', 'BUY', 0, 0
);
INSERT INTO trade_advisories
SELECT * FROM trade_advisories WHERE symbol = '__SCHEMA_TEST__.FR'
ON CONFLICT (symbol, window_start) DO NOTHING;

DO $$
BEGIN
    IF (SELECT count(*) FROM symbols WHERE symbol = '__SCHEMA_TEST__.FR') <> 1
       OR (SELECT count(*) FROM ema_windows WHERE symbol = '__SCHEMA_TEST__.FR') <> 1
       OR (SELECT count(*) FROM trade_advisories WHERE symbol = '__SCHEMA_TEST__.FR') <> 1 THEN
        RAISE EXCEPTION 'replayed results created duplicate rows';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM ema_windows
        WHERE symbol = '__SCHEMA_TEST__.FR'
          AND closing_price_micros = 18446744073709551615
          AND ema38 = 1.234567890123456789012345678901234
          AND ema100 = 0.000000000000000000000000000000001
    ) THEN
        RAISE EXCEPTION 'price or EMA precision was lost';
    END IF;

    BEGIN
        UPDATE ema_windows SET closing_price_micros = 18446744073709551616
        WHERE symbol = '__SCHEMA_TEST__.FR';
        RAISE EXCEPTION 'accepted a price outside the uint64 range';
    EXCEPTION WHEN check_violation THEN NULL;
    END;

    BEGIN
        UPDATE ema_windows SET closing_price_micros = 0 WHERE symbol = '__SCHEMA_TEST__.FR';
        RAISE EXCEPTION 'accepted a zero closing price';
    EXCEPTION WHEN check_violation THEN NULL;
    END;

    BEGIN
        UPDATE ema_windows SET window_end = window_start + INTERVAL '6 minutes'
        WHERE symbol = '__SCHEMA_TEST__.FR';
        RAISE EXCEPTION 'accepted a window longer than five minutes';
    EXCEPTION WHEN check_violation THEN NULL;
    END;

    BEGIN
        UPDATE trade_advisories SET action = 'HOLD' WHERE symbol = '__SCHEMA_TEST__.FR';
        RAISE EXCEPTION 'accepted an unsupported advisory action';
    EXCEPTION WHEN check_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO trade_advisories VALUES (
            '__SCHEMA_TEST__.FR', '2021-11-12 06:05:00+00', 'SELL', 1, 2
        );
        RAISE EXCEPTION 'accepted an advisory without its EMA window';
    EXCEPTION WHEN foreign_key_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO ema_windows VALUES (
            '__SCHEMA_TEST_MISSING__.FR', '2021-11-12 06:00:00+00', '2021-11-12 06:05:00+00', 1, 0, 0
        );
        RAISE EXCEPTION 'accepted an EMA window without its symbol';
    EXCEPTION WHEN foreign_key_violation THEN NULL;
    END;
END;
$$;

ROLLBACK;
