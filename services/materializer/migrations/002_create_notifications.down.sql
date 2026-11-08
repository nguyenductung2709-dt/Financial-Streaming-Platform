BEGIN;

DROP TRIGGER IF EXISTS trade_advisory_notify ON trade_advisories;
DROP TRIGGER IF EXISTS ema_window_notify ON ema_windows;

DROP FUNCTION IF EXISTS notify_trade_advisory_change();
DROP FUNCTION IF EXISTS notify_ema_window_change();

COMMIT;