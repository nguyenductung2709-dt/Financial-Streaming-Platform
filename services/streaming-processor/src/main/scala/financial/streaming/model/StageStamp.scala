package financial.streaming.model

import market.v1.Latency.Stage

/** Wall-clock epoch milliseconds at which a pipeline stage finished for one symbol's window. */
final case class StageStamp(
    symbol: String,
    windowEndMillis: Long,
    stage: Stage,
    atMillis: Long,
    advisoryEmitted: Boolean
)
