package financial.streaming.window

import financial.streaming.serialization.WindowCloseTypeInformation

import financial.streaming.model.WindowClose
import market.v1.Events.PriceEvent
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

final class WindowCloseFunction
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

    out.collect(
      WindowClose(
        closingTrade = closingTrade,
        windowStartMillis = context.window().getStart,
        windowEndMillis = context.window().getEnd
      )
    )
  }
}
