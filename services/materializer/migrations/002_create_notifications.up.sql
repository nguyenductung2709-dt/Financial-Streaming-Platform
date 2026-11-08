BEGIN;

CREATE OR REPLACE FUNCTION notify_ema_window_change()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify(
        'results',
        json_build_object(
            'kind', 'window',
            'symbol', NEW.symbol,
            'window_start', NEW.window_start
        )::text
    );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ema_window_notify
AFTER INSERT OR UPDATE ON ema_windows
FOR EACH ROW
EXECUTE FUNCTION notify_ema_window_change();


CREATE OR REPLACE FUNCTION notify_trade_advisory_change()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify(
        'results',
        json_build_object(
            'kind', 'advisory',
            'symbol', NEW.symbol,
            'window_start', NEW.window_start
        )::text
    );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trade_advisory_notify
AFTER INSERT OR UPDATE ON trade_advisories
FOR EACH ROW
EXECUTE FUNCTION notify_trade_advisory_change();

COMMIT;
