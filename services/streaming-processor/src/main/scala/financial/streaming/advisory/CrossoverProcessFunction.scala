package financial.streaming.advisory

import financial.streaming.latency.LatencyTags
import financial.streaming.model.{CrossoverResult, EmaResult, StageStamp}
import java.math.BigDecimal
import market.v1.Advisory.AdvisoryAction
import market.v1.Latency.Stage
import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/** Apply after keyBy(_.window.symbol). */
final class CrossoverProcessFunction(latencyEnabled: Boolean = false)
    extends KeyedProcessFunction[String, EmaResult, CrossoverResult] {
  @transient private var previousShortEma: ValueState[BigDecimal] = _
  @transient private var previousLongEma: ValueState[BigDecimal] = _

  override def open(context: OpenContext): Unit = {
    previousShortEma = getRuntimeContext.getState(
      new ValueStateDescriptor[BigDecimal]("crossover-ema-38-v1", Types.BIG_DEC)
    )
    previousLongEma = getRuntimeContext.getState(
      new ValueStateDescriptor[BigDecimal]("crossover-ema-100-v1", Types.BIG_DEC)
    )
  }

  override def processElement(
      ema: EmaResult,
      context: KeyedProcessFunction[String, EmaResult, CrossoverResult]#Context,
      out: Collector[CrossoverResult]
  ): Unit = {
    val previousShort = Option(previousShortEma.value()).getOrElse(BigDecimal.ZERO)
    val previousLong = Option(previousLongEma.value()).getOrElse(BigDecimal.ZERO)
    val previousOrder = previousShort.compareTo(previousLong)
    val currentOrder = ema.ema38.compareTo(ema.ema100)

    previousShortEma.update(ema.ema38)
    previousLongEma.update(ema.ema100)

    /* If prevEMA38 <= prevEMA100 && currEMA38 > currEMA100 => BUY advice
      Else if prevEMA38 >= prevEMA100 && currEMA38 < currEMA100 => SELL advice
     */
    val action =
      if (previousOrder <= 0 && currentOrder > 0) Some(AdvisoryAction.ADVISORY_ACTION_BUY)
      else if (previousOrder >= 0 && currentOrder < 0) Some(AdvisoryAction.ADVISORY_ACTION_SELL)
      else None

    if (latencyEnabled)
      context.output(
        LatencyTags.CrossoverEvaluated,
        StageStamp(
          ema.window.symbol,
          ema.window.windowEndMillis,
          Stage.STAGE_CROSSOVER_EVALUATED,
          System.currentTimeMillis(),
          advisoryEmitted = action.isDefined
        )
      )
    action.foreach(a => out.collect(CrossoverResult(ema, a, previousShort, previousLong)))
  }
}
