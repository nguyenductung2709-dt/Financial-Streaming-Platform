package financial.streaming.sink

import financial.streaming.model.CrossoverResult
import java.time.Instant
import org.apache.flink.api.connector.sink2.{Sink, SinkWriter, WriterInitContext}
import org.slf4j.LoggerFactory

final class AdvisoryLoggingSink(limit: Int) extends Sink[CrossoverResult] {
  override def createWriter(context: WriterInitContext): SinkWriter[CrossoverResult] = {
    val counter = context.metricGroup().counter("tradeAdvisories")
    new SinkWriter[CrossoverResult] {
      private var logged = 0
      private val log = LoggerFactory.getLogger(classOf[AdvisoryLoggingSink])

      override def write(result: CrossoverResult, ctx: SinkWriter.Context): Unit = {
        counter.inc()
        if (logged < limit) {
          val ema = result.ema
          val window = ema.window
          log.info(
            "TradeAdvisory symbol={} action={} windowStart={} windowEnd={} ema38={} ema100={} previousEma38={} previousEma100={}",
            window.symbol,
            result.action,
            Instant.ofEpochMilli(window.windowStartMillis).toString,
            Instant.ofEpochMilli(window.windowEndMillis).toString,
            ema.ema38.toPlainString,
            ema.ema100.toPlainString,
            result.previousEma38.toPlainString,
            result.previousEma100.toPlainString
          )
          logged += 1
        }
      }

      override def flush(endOfInput: Boolean): Unit = ()
      override def close(): Unit = ()
    }
  }
}
