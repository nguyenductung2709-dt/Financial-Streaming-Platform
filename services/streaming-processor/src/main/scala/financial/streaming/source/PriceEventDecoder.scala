package financial.streaming.source

import market.v1.Events.{PriceEvent, SecurityType}

object PriceEventDecoder {
  val MaxMessageBytes: Int = 1024 * 1024

  def decode(bytes: Array[Byte]): PriceEvent = {
    require(bytes != null, "tombstone values are not supported on the trade topic")
    require(
      bytes.nonEmpty && bytes.length <= MaxMessageBytes,
      "payload must contain 1 to 1048576 bytes"
    )
    val event = PriceEvent.parseFrom(bytes)
    require(event.getSymbol.trim.nonEmpty, "symbol is empty")
    require(
      event.getSymbol.length <= 256 && !event.getSymbol.exists(Character.isISOControl),
      "invalid symbol"
    )
    require(
      Set(SecurityType.SECURITY_TYPE_EQUITY, SecurityType.SECURITY_TYPE_INDEX)
        .contains(event.getSecurityType),
      "unsupported security type"
    )
    // uint64 values above Long.MaxValue have negative JVM bit representations.
    require(event.getLastPriceMicros != 0L, "price must be positive")
    require(event.getSourceRow != 0L, "source row must be positive")
    require(event.hasTradeTime, "trade timestamp is missing")
    val time = event.getTradeTime
    require(
      time.getSeconds >= -62135596800L && time.getSeconds <= 253402300799L &&
        time.getNanos >= 0 && time.getNanos < 1000000000,
      "invalid trade timestamp"
    )
    event
  }
}
