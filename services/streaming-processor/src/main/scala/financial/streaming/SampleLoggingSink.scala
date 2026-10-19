package financial.streaming

import market.v1.Events.PriceEvent
import org.apache.flink.api.connector.sink2.{Sink, SinkWriter, WriterInitContext}
import org.slf4j.LoggerFactory

// Temporary ingestion diagnostics: count every event and log a limited sample.
// This sink does not persist events; samples are not checkpointed or exactly-once output.
final class SampleLoggingSink(limit: Int) extends Sink[PriceEvent] {
  override def createWriter(context: WriterInitContext): SinkWriter[PriceEvent] = {
    val counter = context.metricGroup().counter("decodedPriceEvents")
    new SinkWriter[PriceEvent] {
      private var logged = 0
      private val log = LoggerFactory.getLogger(classOf[SampleLoggingSink])
      override def write(event: PriceEvent, ctx: SinkWriter.Context): Unit = {
        counter.inc()
        if (logged < limit) {
          log.info(
            "Decoded PriceEvent symbol={} priceMicros={} sourceRow={}",
            event.getSymbol,
            java.lang.Long.toUnsignedString(event.getLastPriceMicros),
            java.lang.Long.toUnsignedString(event.getSourceRow)
          )
          logged += 1
        }
      }
      override def flush(endOfInput: Boolean): Unit = ()
      override def close(): Unit = ()
    }
  }
}
