package financial.streaming.sink

import financial.streaming.model.EmaResult
import java.time.Instant
import org.apache.flink.api.connector.sink2.{Sink, SinkWriter, WriterInitContext}
import org.slf4j.LoggerFactory

// Diagnostic output; log samples are not checkpointed or exactly-once business output.
final class SampleLoggingSink(limit: Int) extends Sink[EmaResult] {
  override def createWriter(context: WriterInitContext): SinkWriter[EmaResult] = {
    val counter = context.metricGroup().counter("completedEmaWindows")
    new SinkWriter[EmaResult] {
      private var logged = 0
      private val log = LoggerFactory.getLogger(classOf[SampleLoggingSink])
      override def write(result: EmaResult, ctx: SinkWriter.Context): Unit = {
        counter.inc()
        if (logged < limit) {
          val message = EmaWindowMapper.toProto(result)
          val start = message.getWindowStart
          val end = message.getWindowEnd
          log.info(
            "EmaWindow symbol={} closingPriceMicros={} windowStart={} windowEnd={} ema38={} ema100={}",
            message.getSymbol,
            java.lang.Long.toUnsignedString(message.getClosingPriceMicros),
            Instant.ofEpochSecond(start.getSeconds, start.getNanos.toLong).toString,
            Instant.ofEpochSecond(end.getSeconds, end.getNanos.toLong).toString,
            message.getEma38,
            message.getEma100
          )
          logged += 1
        }
      }
      override def flush(endOfInput: Boolean): Unit = ()
      override def close(): Unit = ()
    }
  }
}
