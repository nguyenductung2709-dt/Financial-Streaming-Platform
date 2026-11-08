BEGIN;

DROP TRIGGER trade_advisory_revision ON trade_advisories;
DROP TRIGGER ema_window_revision ON ema_windows;
DROP FUNCTION advance_result_revision();
ALTER TABLE trade_advisories DROP COLUMN revision;
ALTER TABLE ema_windows DROP COLUMN revision;
DROP SEQUENCE result_revision_seq;

COMMIT;
