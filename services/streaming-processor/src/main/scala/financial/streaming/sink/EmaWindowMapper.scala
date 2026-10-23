package financial.streaming.sink

import com.google.protobuf.Timestamp
import financial.streaming.model.EmaResult
import market.v1.Ema.EmaWindow

object EmaWindowMapper {
  def toProto(result: EmaResult): EmaWindow =
    EmaWindow
      .newBuilder()
      .setSymbol(result.window.symbol)
      .setWindowStart(timestamp(result.window.windowStartMillis))
      .setWindowEnd(timestamp(result.window.windowEndMillis))
      .setClosingPriceMicros(result.window.closingTrade.getLastPriceMicros)
      .setEma38(result.ema38.toPlainString)
      .setEma100(result.ema100.toPlainString)
      .build()

  private def timestamp(millis: Long): Timestamp =
    Timestamp
      .newBuilder()
      .setSeconds(Math.floorDiv(millis, 1000L))
      .setNanos(Math.floorMod(millis, 1000L).toInt * 1000000)
      .build()
}
