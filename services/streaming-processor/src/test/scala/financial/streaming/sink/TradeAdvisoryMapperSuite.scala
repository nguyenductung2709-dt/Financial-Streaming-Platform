package financial.streaming.sink

import financial.streaming.model.{CrossoverResult, EmaResult, WindowClose}
import java.math.BigDecimal
import market.v1.Advisory.{AdvisoryAction, TradeAdvisory}
import market.v1.Events.PriceEvent

class TradeAdvisoryMapperSuite extends munit.FunSuite {
  private val result = CrossoverResult(
    EmaResult(
      WindowClose(
        PriceEvent.newBuilder().setSymbol("IUSAC.FR").setLastPriceMicros(-1L).build(),
        1636693200123L,
        1636693500123L
      ),
      new BigDecimal("21.44666666666666666666666666666667"),
      new BigDecimal("8.281386138613861386138613861386139")
    ),
    AdvisoryAction.ADVISORY_ACTION_BUY,
    new BigDecimal("7.000000000000000000000000000000001"),
    new BigDecimal("8.00")
  )

  test("map the complete EMA window, action and previous values through a Protobuf round trip") {
    val sell = result.copy(
      action = AdvisoryAction.ADVISORY_ACTION_SELL,
      ema = result.ema.copy(ema38 = new BigDecimal("6")),
      previousEma38 = new BigDecimal("9")
    )
    Seq(result, sell).foreach { input =>
      val message = TradeAdvisoryMapper.toProto(input)
      assertEquals(message.getEmaWindow, EmaWindowMapper.toProto(input.ema))
      assertEquals(message.getAction, input.action)
      assertEquals(message.getPreviousEma38, input.previousEma38.toPlainString)
      assertEquals(message.getPreviousEma100, "8.00")
      val decoded = TradeAdvisory.parseFrom(message.toByteArray)
      assertEquals(decoded, message)
      assertEquals(decoded.getEmaWindow.getSymbol, "IUSAC.FR")
      assertEquals(decoded.getEmaWindow.getClosingPriceMicros, -1L)
      assertEquals(decoded.getEmaWindow.getWindowStart.getNanos, 123000000)
      assertEquals(decoded.getEmaWindow.getWindowEnd.getNanos, 123000000)
    }
  }

  test("previous EMA values preserve exact decimals without exponent notation") {
    val message = TradeAdvisoryMapper.toProto(
      result.copy(
        previousEma38 = new BigDecimal("1E+12"),
        previousEma100 = new BigDecimal("1E-12")
      )
    )
    assertEquals(message.getPreviousEma38, "1000000000000")
    assertEquals(message.getPreviousEma100, "0.000000000001")
  }

  test("the initial advisory includes both previous EMA values as zero") {
    val message = TradeAdvisoryMapper.toProto(
      result.copy(
        previousEma38 = BigDecimal.ZERO,
        previousEma100 = BigDecimal.ZERO
      )
    )
    assertEquals(message.getPreviousEma38, "0")
    assertEquals(message.getPreviousEma100, "0")
    assertEquals(message.getAction, AdvisoryAction.ADVISORY_ACTION_BUY)
  }
}
