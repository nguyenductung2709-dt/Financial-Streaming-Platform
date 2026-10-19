package financial.streaming.model

import market.v1.Events.PriceEvent

/** A completed window's closing trade and epoch-millisecond boundaries: [start, end). */
final case class WindowClose(
    closingTrade: PriceEvent,
    windowStartMillis: Long,
    windowEndMillis: Long
) {
  def symbol: String = closingTrade.getSymbol
}
