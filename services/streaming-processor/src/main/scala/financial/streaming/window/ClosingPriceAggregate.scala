package financial.streaming.window

import market.v1.Events.PriceEvent
import org.apache.flink.api.common.functions.AggregateFunction

final class ClosingPriceAggregate extends AggregateFunction[PriceEvent, PriceEvent, PriceEvent] {
  override def createAccumulator(): PriceEvent = PriceEvent.getDefaultInstance

  override def getResult(accumulator: PriceEvent): PriceEvent = accumulator

  override def add(event: PriceEvent, accumulator: PriceEvent): PriceEvent =
    selectLatest(event, accumulator)

  override def merge(first: PriceEvent, second: PriceEvent): PriceEvent =
    selectLatest(first, second)

  private def selectLatest(
      first: PriceEvent,
      second: PriceEvent
  ): PriceEvent = {
    if (!first.hasTradeTime) {
      second
    } else if (!second.hasTradeTime) {
      first
    } else {
      val firstTime = first.getTradeTime
      val secondTime = second.getTradeTime

      val secondsComparison =
        java.lang.Long.compare(firstTime.getSeconds, secondTime.getSeconds)

      val timestampComparison =
        if (secondsComparison != 0) secondsComparison
        else java.lang.Integer.compare(firstTime.getNanos, secondTime.getNanos)

      if (timestampComparison > 0) {
        first
      } else if (timestampComparison < 0) {
        second
      } else if (java.lang.Long.compareUnsigned(first.getSourceRow, second.getSourceRow) > 0) {
        first
      } else {
        second
      }
    }
  }
}
