BEGIN;

-- A shared sequence lets an advisory include changes to its joined EMA window.
-- It is a per-row freshness marker, not a transaction/commit replay cursor.
CREATE SEQUENCE result_revision_seq;
ALTER TABLE ema_windows ADD COLUMN revision BIGINT NOT NULL DEFAULT nextval('result_revision_seq');
ALTER TABLE trade_advisories ADD COLUMN revision BIGINT NOT NULL DEFAULT nextval('result_revision_seq');

CREATE FUNCTION advance_result_revision() RETURNS TRIGGER AS $$
BEGIN
    NEW.revision := nextval('result_revision_seq');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ema_window_revision BEFORE UPDATE ON ema_windows
FOR EACH ROW EXECUTE FUNCTION advance_result_revision();
CREATE TRIGGER trade_advisory_revision BEFORE UPDATE ON trade_advisories
FOR EACH ROW EXECUTE FUNCTION advance_result_revision();

COMMIT;
