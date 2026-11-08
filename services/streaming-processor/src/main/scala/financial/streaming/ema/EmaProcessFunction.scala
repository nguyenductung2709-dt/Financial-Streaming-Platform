package financial.streaming.ema

import financial.streaming.latency.LatencyTags
import financial.streaming.model.{EmaResult, StageStamp, WindowClose}
import java.math.{BigDecimal, MathContext}
import market.v1.Latency.Stage
import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/** Apply after keyBy(symbol). Flink keeps separate EMA state for each symbol. */
final class EmaProcessFunction(latencyEnabled: Boolean = false)
    extends KeyedProcessFunction[String, WindowClose, EmaResult] {
  @transient private var shortEma: ValueState[BigDecimal] = _
  @transient private var longEma: ValueState[BigDecimal] = _

  override def open(context: OpenContext): Unit = {
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
    val priceMicros = window.closingTrade.getLastPriceMicros
    require(priceMicros != 0L, "closing price must be positive")
    // Protobuf uint64 uses signed Long bits on the JVM; convert through unsigned text.
    val close = new BigDecimal(java.lang.Long.toUnsignedString(priceMicros)).movePointLeft(6)
    val nextShort = next(shortEma.value(), close, period = 38)
    val nextLong = next(longEma.value(), close, period = 100)

    shortEma.update(nextShort)
    longEma.update(nextLong)
    if (latencyEnabled)
      context.output(
        LatencyTags.EmaComputed,
        StageStamp(
          window.symbol,
          window.windowEndMillis,
          Stage.STAGE_EMA_COMPUTED,
          System.currentTimeMillis(),
          advisoryEmitted = false
        )
      )
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
