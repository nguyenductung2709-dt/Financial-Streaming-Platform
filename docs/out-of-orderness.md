# Why the watermark allows 1 second

The processor's watermark trails the highest trade time it has seen by 1 second
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

## Why 1 second

The smallest allowance that keeps every real trade is 264 ms. We use 1 second, about
four times that. On all five days it drops exactly the same events as 5 or 70 seconds
would, so the results are identical, and every window closes sooner.

We first chose 5 seconds, to leave room for live feeds from three exchanges that could
arrive a second or more apart. This project only ever replays the recorded files,
though, and in a replay the worst case is the 264 ms above. The extra four seconds
only added waiting time to every window. That cost is easy to see in the latency runs
at low rates: at 5,000 events/s, the median wait for the watermark was 701 ms with
5 seconds and 513 ms with 1 second. It's smaller than five times less because most
of the wait falls in the first second after a window ends. At 50,000 events/s and
above, the wait is under 70 ms either way.

The risk of going lower is still the same. If a day turned out to be worse than
the five we measured, an allowance that's too small would quietly corrupt that
symbol's EMAs from then on. If you replay other data, or connect a live feed, measure
its lateness first and raise the allowance if needed.
