package financial.streaming.sink

import financial.streaming.model.{EmaResult, WindowClose}
import java.math.BigDecimal
import market.v1.Ema.EmaWindow
import market.v1.Events.PriceEvent

class EmaWindowMapperSuite extends munit.FunSuite {
  private def result(start: Long, price: Long, short: String, long: String): EmaResult =
    EmaResult(
      WindowClose(
        PriceEvent
          .newBuilder()
          .setSymbol("IUSAC.FR")
          .setLastPriceMicros(price)
          .build(),
        start,
        start + 300000L
      ),
      new BigDecimal(short),
      new BigDecimal(long)
    )

  test("map every output field and preserve milliseconds through a Protobuf round trip") {
    val message = EmaWindowMapper.toProto(
      result(
        1636693200123L,
        418210000L,
        "21.44666666666666666666666666666667",
        "8.281386138613861386138613861386139"
      )
    )
    assertEquals(message.getSymbol, "IUSAC.FR")
    assertEquals(message.getClosingPriceMicros, 418210000L)
    assertEquals(message.getWindowStart.getSeconds, 1636693200L)
    assertEquals(message.getWindowStart.getNanos, 123000000)
    assertEquals(message.getWindowEnd.getSeconds, 1636693500L)
    assertEquals(message.getWindowEnd.getNanos, 123000000)
    assertEquals(message.getEma38, "21.44666666666666666666666666666667")
    assertEquals(message.getEma100, "8.281386138613861386138613861386139")
    assertEquals(EmaWindow.parseFrom(message.toByteArray), message)
  }

  test("unsigned prices and decimal values keep precision without exponent notation") {
    val message = EmaWindowMapper.toProto(result(0L, -1L, "1E+12", "1E-12"))
    assertEquals(
      java.lang.Long.toUnsignedString(message.getClosingPriceMicros),
      "18446744073709551615"
    )
    assertEquals(message.getEma38, "1000000000000")
    assertEquals(message.getEma100, "0.000000000001")
  }

  test("timestamps before the Unix epoch use nonnegative nanoseconds") {
    val message = EmaWindowMapper.toProto(result(-1L, 1L, "0", "0"))
    assertEquals(message.getWindowStart.getSeconds, -1L)
    assertEquals(message.getWindowStart.getNanos, 999000000)
    assertEquals(message.getWindowEnd.getSeconds, 299L)
    assertEquals(message.getWindowEnd.getNanos, 999000000)
  }
}
