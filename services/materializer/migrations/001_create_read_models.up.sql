BEGIN;

CREATE TABLE symbols (
    symbol TEXT PRIMARY KEY
);

CREATE TABLE ema_windows (
    symbol TEXT NOT NULL REFERENCES symbols(symbol),
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    closing_price_micros NUMERIC(20, 0) NOT NULL,
    ema38 NUMERIC NOT NULL,
    ema100 NUMERIC NOT NULL,

    PRIMARY KEY (symbol, window_start),

    CHECK (window_end = window_start + INTERVAL '5 minutes'),
    CHECK (
        closing_price_micros > 0
        AND closing_price_micros <= 18446744073709551615
    )
);

CREATE TABLE trade_advisories (
    symbol TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('BUY', 'SELL')),
    previous_ema38 NUMERIC NOT NULL,
    previous_ema100 NUMERIC NOT NULL,

    PRIMARY KEY (symbol, window_start),

    FOREIGN KEY (symbol, window_start)
        REFERENCES ema_windows(symbol, window_start)
);

COMMIT;
