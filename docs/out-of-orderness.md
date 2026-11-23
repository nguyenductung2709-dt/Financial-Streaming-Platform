# Why the watermark allows 5 seconds

The processor's watermark trails the highest trade time it has seen by 5 seconds
(`OutOfOrdernessAllowance` in `StreamingProcessor.scala`). A window fires when the
watermark passes its end, and anything that arrives after that is dropped. The
validation and latency tools use the same value from `OUT_OF_ORDERNESS_MS` in
`validation/config.py`, so if you change one, change the other.

Choosing the allowance means trading correctness against latency. If it's too small, a
late trade is dropped without any error. That window's closing price is then wrong, and
because each EMA builds on the one before, every later window of that symbol is wrong
too. In one three-day run, 5,506 dropped events changed 229,999 windows. If it's too
large, every result waits that much longer. During a replay the wait passes in
milliseconds, but on a live feed it would be real seconds before each EMA and advisory.

## What the data needs

For every trade, we measured how far the watermark had already passed its window when
the trade arrived, reading the files in order. File order is also topic order. Across
all five trading days, about 59.4 million trades:

| Day | Trades that need any allowance | Largest needed |
| --- | --- | --- |
| 08-11-21 | 79 | 25 ms |
| 09-11-21 | 76 | 121 ms |
| 10-11-21 | 54 | 15 ms |
| 11-11-21 | 119 | 264 ms |
| 12-11-21 | 24 | 18 ms |

These are trades stamped just before a window boundary that arrive a few milliseconds
after a newer trade from another exchange, mostly Paris (`.FR`) and Amsterdam (`.NL`)
symbols. That's normal clock skew between exchange feeds. With no allowance at all, 352
real trades would be dropped. At 1 second or more, none are. The weekend files (13-11
and 14-11) contain no trades.

Two other things look like bigger lateness but aren't. Every day, about 430 trades from
the Xetra closing auction are stamped 17:30 and arrive up to 65 seconds behind the
newest trade. Their 17:30 window is still open at that point, though, so they're kept
whatever the allowance is. An earlier version used 70 seconds precisely to cover this
gap. And on 10-11-21 there are 5,506 placeholder index rows stamped exactly 01:00:00.000
with no date, which show up around 18:56 in the file. They're about 18 hours late, so
every allowance drops them, and `compare-flow` reports the affected windows as
`lateness_explained`.

## Why 5 and not 0.3

The smallest allowance that keeps every real trade is 264 ms. We picked 5 seconds,
about twenty times that, for three reasons. First, the worst case varies a lot: it
ranges from 15 ms to 264 ms over just five days, so we want headroom for worse days.
Second, a replay is the best case there is, one ordered file in one partition. Live
feeds from three exchanges arriving over a network can easily be a second or more
apart. Third, the two kinds of mistake don't cost the same. An allowance that's too
small quietly corrupts a symbol's EMAs from then on, while each extra second only delays
results by a second. 5 seconds is under 2% of a five-minute window.

On this dataset, 5 seconds and 70 seconds drop exactly the same events and give
identical results. With 5 seconds, a live result arrives 65 seconds sooner.
