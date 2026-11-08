BEGIN;

DROP TRIGGER IF EXISTS symbol_notify ON symbols;
DROP FUNCTION IF EXISTS notify_symbol_change();

COMMIT;
