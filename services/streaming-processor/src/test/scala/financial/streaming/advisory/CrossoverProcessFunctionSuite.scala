package financial.streaming.advisory

import financial.streaming.model.{CrossoverResult, EmaResult, WindowClose}
import financial.streaming.serialization.CrossoverResultSerializer
import java.math.BigDecimal
import market.v1.Advisory.AdvisoryAction
import market.v1.Events.PriceEvent
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import scala.jdk.CollectionConverters._

class CrossoverProcessFunctionSuite extends munit.FunSuite {
  private type Harness = KeyedOneInputStreamOperatorTestHarness[String, EmaResult, CrossoverResult]

  private def harness(): Harness = {
    val h = new Harness(
      new KeyedProcessOperator[String, EmaResult, CrossoverResult](new CrossoverProcessFunction),
      new KeySelector[EmaResult, String] {
        override def getKey(ema: EmaResult): String = ema.window.symbol
      },
      Types.STRING
    )
    h.setup(new CrossoverResultSerializer)
    h
  }

  private def withHarness(test: Harness => Unit): Unit = {
    val h = harness()
    try {
      h.open()
      test(h)
    } finally h.close()
  }

  private def ema(
      short: String,
      long: String,
      start: Long = 0L,
      symbol: String = "RUI.FR"
  ): EmaResult = {
    val trade = PriceEvent.newBuilder().setSymbol(symbol).setLastPriceMicros(39000000L).build()
    EmaResult(
      WindowClose(trade, start, start + 300000L),
      new BigDecimal(short),
      new BigDecimal(long)
    )
  }

  private def emit(h: Harness, value: EmaResult): List[CrossoverResult] = {
    h.getOutput.clear()
    h.processElement(new StreamRecord(value))
    h.extractOutputValues().asScala.toList
  }

  test("the first positive EMA pair produces BUY from zero and preserves the input") {
    withHarness { h =>
      val input = ema("2", "0.7722772277227722772277227722772277")
      assertEquals(
        emit(h, input),
        List(
          CrossoverResult(
            input,
            AdvisoryAction.ADVISORY_ACTION_BUY,
            BigDecimal.ZERO,
            BigDecimal.ZERO
          )
        )
      )
    }
  }

  test("crossing from below to above produces BUY with the previous EMA pair") {
    withHarness { h =>
      emit(h, ema("1", "2"))
      val input = ema("3", "2", start = 300000L)
      assertEquals(
        emit(h, input),
        List(
          CrossoverResult(
            input,
            AdvisoryAction.ADVISORY_ACTION_BUY,
            new BigDecimal("1"),
            new BigDecimal("2")
          )
        )
      )
    }
  }

  test("crossing from above to below produces SELL with the previous EMA pair") {
    withHarness { h =>
      emit(h, ema("3", "2"))
      val input = ema("1", "2", start = 300000L)
      assertEquals(
        emit(h, input),
        List(
          CrossoverResult(
            input,
            AdvisoryAction.ADVISORY_ACTION_SELL,
            new BigDecimal("3"),
            new BigDecimal("2")
          )
        )
      )
    }
  }

  test("current equality emits nothing and previous equality permits either direction") {
    withHarness { h =>
      emit(h, ema("3", "2"))
      assertEquals(emit(h, ema("2.0", "2.00", start = 300000L)), Nil)
      assertEquals(
        emit(h, ema("1.999999999999999999999999999999999", "2", start = 600000L)).map(_.action),
        List(AdvisoryAction.ADVISORY_ACTION_SELL)
      )
      assertEquals(emit(h, ema("2.00", "2.0", start = 900000L)), Nil)
      assertEquals(
        emit(h, ema("2.000000000000000000000000000000001", "2", start = 1200000L)).map(_.action),
        List(AdvisoryAction.ADVISORY_ACTION_BUY)
      )
    }
  }

  test("remaining on the same side emits nothing but still updates previous values") {
    withHarness { h =>
      emit(h, ema("3", "2"))
      assertEquals(emit(h, ema("4", "2", start = 300000L)), Nil)
      assertEquals(emit(h, ema("4", "2", start = 600000L)), Nil)
      val sell = emit(h, ema("1", "2", start = 900000L)).head
      assertEquals(sell.previousEma38, new BigDecimal("4"))
      assertEquals(sell.action, AdvisoryAction.ADVISORY_ACTION_SELL)
      assertEquals(emit(h, ema("0.5", "2", start = 1200000L)), Nil)
      val buy = emit(h, ema("3", "2", start = 1500000L)).head
      assertEquals(buy.previousEma38, new BigDecimal("0.5"))
      assertEquals(buy.action, AdvisoryAction.ADVISORY_ACTION_BUY)
    }
  }

  test("symbols including different exchanges maintain independent previous values") {
    withHarness { h =>
      emit(h, ema("3", "2", symbol = "RUI.FR"))
      emit(h, ema("1", "2", symbol = "RUI.ETR"))
      assertEquals(emit(h, ema("4", "2", start = 300000L, symbol = "RUI.FR")), Nil)
      assertEquals(
        emit(h, ema("3", "2", start = 300000L, symbol = "RUI.ETR")).map(_.action),
        List(AdvisoryAction.ADVISORY_ACTION_BUY)
      )
    }
  }

  test("a gap across days retains previous values") {
    withHarness { h =>
      emit(h, ema("3", "2"))
      assertEquals(emit(h, ema("4", "2", start = 86400000L)), Nil)
      assertEquals(
        emit(h, ema("1", "2", start = 86700000L)).map(_.action),
        List(AdvisoryAction.ADVISORY_ACTION_SELL)
      )
    }
  }

  test("checkpoint restoration preserves both symbols and the latest non-signal window") {
    val original = harness()
    try {
      original.open()
      emit(original, ema("3", "2", symbol = "RUI.FR"))
      emit(original, ema("4", "2", start = 300000L, symbol = "RUI.FR"))
      emit(original, ema("1", "2", symbol = "RUI.ETR"))
      val checkpoint = original.snapshot(1L, 1L)
      try {
        val inputs = List(
          ema("5", "2", start = 600000L, symbol = "RUI.FR"),
          ema("1", "2", start = 900000L, symbol = "RUI.FR"),
          ema("3", "2", start = 300000L, symbol = "RUI.ETR")
        )
        val expected = inputs.map(emit(original, _))
        val restored = harness()
        try {
          restored.initializeState(checkpoint)
          restored.open()
          assertEquals(inputs.map(emit(restored, _)), expected)
          assertEquals(expected.head, Nil)
          assertEquals(expected(1).head.previousEma38, new BigDecimal("5"))
          assertEquals(expected(2).head.previousEma38, new BigDecimal("1"))
        } finally restored.close()
      } finally checkpoint.discardState()
    } finally original.close()
  }

}
