package financial.streaming.window

import financial.streaming.serialization.WindowCloseTypeInformation

import financial.streaming.latency.LatencyTags
import financial.streaming.model.{StageStamp, WindowClose}
import market.v1.Events.PriceEvent
import market.v1.Latency.Stage
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

final class WindowCloseFunction(latencyEnabled: Boolean = false)
    extends ProcessWindowFunction[PriceEvent, WindowClose, String, TimeWindow]
    with ResultTypeQueryable[WindowClose] {

  override def getProducedType: TypeInformation[WindowClose] =
    new WindowCloseTypeInformation

  override def process(
      symbol: String,
      context: ProcessWindowFunction[
        PriceEvent,
        WindowClose,
        String,
        TimeWindow
      ]#Context,
      elements: java.lang.Iterable[PriceEvent],
      out: Collector[WindowClose]
  ): Unit = {
    val closingTrade = elements.iterator().next()

    val window = WindowClose(
      closingTrade = closingTrade,
      windowStartMillis = context.window().getStart,
      windowEndMillis = context.window().getEnd
    )
    if (latencyEnabled)
      context.output(
        LatencyTags.WindowFired,
        StageStamp(
          symbol,
          window.windowEndMillis,
          Stage.STAGE_WINDOW_FIRED,
          System.currentTimeMillis(),
          advisoryEmitted = false
        )
      )
    out.collect(window)
  }
}
