package financial.streaming.ema

import com.google.protobuf.Timestamp
import financial.streaming.model.{EmaResult, WindowClose}
import java.math.BigDecimal
import market.v1.Events.{PriceEvent, SecurityType}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import scala.jdk.CollectionConverters._

class EmaProcessFunctionSuite extends munit.FunSuite {
  private type Harness = KeyedOneInputStreamOperatorTestHarness[String, WindowClose, EmaResult]

  private def harness(): Harness = {
    val h = new Harness(
      new KeyedProcessOperator[String, WindowClose, EmaResult](new EmaProcessFunction),
      new KeySelector[WindowClose, String] {
        override def getKey(window: WindowClose): String = window.symbol
      },
      Types.STRING
    )
    // The harness copies emitted objects. These models are immutable, so an identity copy
    // isolates operator/state tests from output serialization, which will be connected later.
    h.setup(new OutputCopySerializer)
    h
  }

  private def withHarness(test: Harness => Unit): Unit = {
    val h = harness()
    try {
      h.open()
      test(h)
    } finally h.close()
  }

  private def window(symbol: String, start: Long, priceMicros: Long): WindowClose = {
    val trade = PriceEvent
      .newBuilder()
      .setSymbol(symbol)
      .setSecurityType(SecurityType.SECURITY_TYPE_EQUITY)
      .setLastPriceMicros(priceMicros)
      .setSourceRow(14L)
      .setTradeTime(Timestamp.newBuilder().setSeconds(start / 1000L))
      .build()
    WindowClose(trade, start, start + 300000L)
  }

  private def emit(h: Harness, value: WindowClose): EmaResult = {
    h.getOutput.clear()
    h.processElement(new StreamRecord(value))
    val results = h.extractOutputValues().asScala.toList
    assertEquals(results.size, 1)
    results.head
  }

  private def assertDecimal(actual: BigDecimal, expected: String): Unit =
    assertEquals(
      actual.compareTo(new BigDecimal(expected)),
      0,
      s"actual=$actual expected=$expected"
    )

  test("the first window starts both EMA values from zero and preserves its source window") {
    withHarness { h =>
      val input = window("RUI.FR", 0L, 39000000L)
      val result = emit(h, input)
      assertEquals(result.window, input)
      assertDecimal(result.ema38, "2") // 39 * 2/39
      assertDecimal(result.ema100, "0.7722772277227722772277227722772277") // 78/101
    }
  }

  test("successive windows use saved EMA values for rising and falling prices") {
    // Independent reference values, rounding each window update to 34 significant digits.
    val fixtures = List(
      (39000000L, "2", "0.7722772277227722772277227722772277"),
      (78000000L, "5.897435897435897435897435897435897", "2.301539064797568865797470836192530"),
      (19500000L, "6.595003287310979618671926364234056", "2.642102647672864531821283294881787"),
      (28040000L, "7.694746708474519125406699371196412", "3.145031308114986026240663823696009")
    )
    withHarness { h =>
      fixtures.zipWithIndex.foreach { case ((price, short, long), index) =>
        val result = emit(h, window("RUI.FR", index * 300000L, price))
        assertDecimal(result.ema38, short)
        assertDecimal(result.ema100, long)
      }
    }
  }

  test("interleaved symbols including separate exchanges have independent EMA state") {
    withHarness { h =>
      emit(h, window("RUI.FR", 0L, 39000000L))
      val other = emit(h, window("RUI.ETR", 0L, 78000000L))
      assertDecimal(other.ema38, "4")
      assertDecimal(other.ema100, "1.544554455445544554455445544554455")
      val next = emit(h, window("RUI.FR", 300000L, 78000000L))
      assertDecimal(next.ema38, "5.897435897435897435897435897435897")
      assertDecimal(next.ema100, "2.301539064797568865797470836192530")
    }
  }

  test("checkpoint restoration produces the same continuation as uninterrupted processing") {
    val original = harness()
    try {
      original.open()
      emit(original, window("RUI.FR", 0L, 39000000L))
      emit(original, window("RUI.ETR", 0L, 78000000L))
      val checkpoint = original.snapshot(1L, 1L)
      try {
        val nextA = window("RUI.FR", 300000L, 78000000L)
        val nextB = window("RUI.ETR", 300000L, 39000000L)
        val expectedA = emit(original, nextA)
        val expectedB = emit(original, nextB)
        val restored = harness()
        try {
          restored.initializeState(checkpoint)
          restored.open()
          assertEquals(emit(restored, nextA), expectedA)
          assertEquals(emit(restored, nextB), expectedB)
        } finally restored.close()
      } finally checkpoint.discardState()
    } finally original.close()
  }

  test("micros and unsigned uint64 prices are interpreted without float conversion") {
    withHarness { h =>
      val tiny = emit(h, window("TINY.FR", 0L, 39L))
      assertDecimal(tiny.ema38, "0.000002")
      val fractional = emit(h, window("FRACTION.FR", 0L, 39000039L))
      assertDecimal(fractional.ema38, "2.000002")
      val maximum = emit(h, window("MAX.FR", 0L, -1L)) // Protobuf uint64 maximum
      assertDecimal(maximum.ema38, "945986875574.8488007692307692307692")
      assertDecimal(maximum.ema100, "365282060865.5356755445544554455446")
    }
  }

  test("empty-window gaps retain the previous values without extra EMA updates") {
    withHarness { h =>
      emit(h, window("RUI.FR", 0L, 39000000L))
      val afterGap = emit(h, window("RUI.FR", 900000L, 78000000L))
      assertDecimal(afterGap.ema38, "5.897435897435897435897435897435897")
      assertDecimal(afterGap.ema100, "2.301539064797568865797470836192530")
    }
  }

  test("a zero closing price fails without emitting a result or changing EMA state") {
    withHarness { h =>
      emit(h, window("RUI.FR", 0L, 39000000L))
      h.getOutput.clear()
      intercept[IllegalArgumentException] {
        h.processElement(new StreamRecord(window("RUI.FR", 300000L, 0L)))
      }
      assert(h.getOutput.isEmpty)
      val next = emit(h, window("RUI.FR", 300000L, 78000000L))
      assertDecimal(next.ema38, "5.897435897435897435897435897435897")
      assertDecimal(next.ema100, "2.301539064797568865797470836192530")
    }
  }

  // Output objects stay in memory in these tests; managed state still uses real Flink serializers.
  private class OutputCopySerializer extends TypeSerializerSingleton[EmaResult] {
    override def isImmutableType: Boolean = true
    override def createInstance(): EmaResult = unsupported()
    override def copy(value: EmaResult): EmaResult = value
    override def copy(value: EmaResult, reuse: EmaResult): EmaResult = value
    override def getLength: Int = -1
    override def serialize(value: EmaResult, target: DataOutputView): Unit = unsupported()
    override def deserialize(source: DataInputView): EmaResult = unsupported()
    override def deserialize(reuse: EmaResult, source: DataInputView): EmaResult = unsupported()
    override def copy(source: DataInputView, target: DataOutputView): Unit = unsupported()
    override def snapshotConfiguration(): TypeSerializerSnapshot[EmaResult] = unsupported()
    private def unsupported(): Nothing = throw new UnsupportedOperationException(
      "Output serialization is outside these operator tests"
    )
  }
}
