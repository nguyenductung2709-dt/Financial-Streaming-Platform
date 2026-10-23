package financial.streaming

import financial.streaming.model.WindowClose
import org.apache.flink.api.connector.sink2.{Sink, SinkWriter, WriterInitContext}
import org.slf4j.LoggerFactory

// Temporary window diagnostics: count every completed window and log a limited sample.
// This sink does not persist events; samples are not checkpointed or exactly-once output.
final class SampleLoggingSink(limit: Int) extends Sink[WindowClose] {
  override def createWriter(context: WriterInitContext): SinkWriter[WindowClose] = {
    val counter = context.metricGroup().counter("completedPriceWindows")
    new SinkWriter[WindowClose] {
      private var logged = 0
      private val log = LoggerFactory.getLogger(classOf[SampleLoggingSink])
      override def write(window: WindowClose, ctx: SinkWriter.Context): Unit = {
        val event = window.closingTrade
        counter.inc()
        if (logged < limit) {
          log.info(
            "WindowClose symbol={} priceMicros={} sourceRow={} windowStartMillis={} windowEndMillis={}",
            event.getSymbol,
            java.lang.Long.toUnsignedString(event.getLastPriceMicros),
            java.lang.Long.toUnsignedString(event.getSourceRow),
            Long.box(window.windowStartMillis),
            Long.box(window.windowEndMillis)
          )
          logged += 1
        }
      }
      override def flush(endOfInput: Boolean): Unit = ()
      override def close(): Unit = ()
    }
  }
}
