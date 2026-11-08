BEGIN;

CREATE OR REPLACE FUNCTION notify_symbol_change()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify(
        'results',
        json_build_object(
            'kind', 'symbol',
            'symbol', NEW.symbol
        )::text
    );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER symbol_notify
AFTER INSERT ON symbols
FOR EACH ROW
EXECUTE FUNCTION notify_symbol_change();

COMMIT;
