package financial.streaming.ema

import financial.streaming.model.{EmaResult, WindowClose}
import java.math.{BigDecimal, MathContext}
import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/** Apply after keyBy(symbol). Flink keeps separate EMA state for each symbol. */
final class EmaProcessFunction extends KeyedProcessFunction[String, WindowClose, EmaResult] {
  @transient private var shortEma: ValueState[BigDecimal] = _
  @transient private var longEma: ValueState[BigDecimal] = _

  @transient private var lastWindowEnd: ValueState[java.lang.Long] = _

  override def open(context: OpenContext): Unit = {
    lastWindowEnd = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("ema-last-window-end-v1", Types.LONG)
    )
    shortEma = getRuntimeContext.getState(
      new ValueStateDescriptor[BigDecimal]("ema-38-v1", Types.BIG_DEC)
    )
    longEma = getRuntimeContext.getState(
      new ValueStateDescriptor[BigDecimal]("ema-100-v1", Types.BIG_DEC)
    )
  }

  override def processElement(
      window: WindowClose,
      context: KeyedProcessFunction[String, WindowClose, EmaResult]#Context,
      out: Collector[EmaResult]
  ): Unit = {
    val previousEnd = lastWindowEnd.value()
    val previousShort = shortEma.value()
    val previousLong = longEma.value()
    if (previousEnd == null && (previousShort != null || previousLong != null)) {
      throw new IllegalStateException(
        "Checkpoint lacks EMA window progress; use a fresh replay or migrate the legacy state"
      )
    }
    // Producer replay after restore can recreate windows that were already applied.
    if (previousEnd != null && window.windowEndMillis <= previousEnd.longValue()) return

    val priceMicros = window.closingTrade.getLastPriceMicros
    require(priceMicros != 0L, "closing price must be positive")
    // Protobuf uint64 uses signed Long bits on the JVM; convert through unsigned text.
    val close = new BigDecimal(java.lang.Long.toUnsignedString(priceMicros)).movePointLeft(6)
    val nextShort = next(previousShort, close, period = 38)
    val nextLong = next(previousLong, close, period = 100)

    shortEma.update(nextShort)
    longEma.update(nextLong)
    lastWindowEnd.update(java.lang.Long.valueOf(window.windowEndMillis))
    out.collect(EmaResult(window, nextShort, nextLong))
  }

  private def next(previous: BigDecimal, close: BigDecimal, period: Int): BigDecimal = {
    val previousEma = Option(previous).getOrElse(BigDecimal.ZERO)
    // Equivalent to close * alpha + previous * (1 - alpha), alpha = 2 / (period + 1).
    // Decimal division uses 34 significant digits with HALF_EVEN rounding.
    close
      .multiply(BigDecimal.valueOf(2))
      .add(previousEma.multiply(BigDecimal.valueOf(period - 1L)))
      .divide(BigDecimal.valueOf(period + 1L), MathContext.DECIMAL128)
  }
}
