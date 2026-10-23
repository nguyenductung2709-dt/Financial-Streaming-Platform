package financial.streaming.window

import financial.streaming.serialization.PriceEventTypeInformation
import financial.streaming.StreamingProcessor

import com.google.protobuf.Timestamp
import financial.streaming.model.WindowClose
import market.v1.Events.{PriceEvent, SecurityType}
import org.apache.flink.configuration.{Configuration, PipelineOptions}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import java.time.Instant
import scala.jdk.CollectionConverters._

class ClosingPriceWindowSuite extends munit.FunSuite {
  override val munitTimeout: scala.concurrent.duration.Duration =
    scala.concurrent.duration.Duration(60, "seconds")

  private val start = Instant.parse("2021-11-12T05:00:00Z").toEpochMilli // 07:00 fixed CEST
  private val fiveMinutes = 300000L

  private def trade(symbol: String, millis: Long, row: Long, nanosExtra: Int = 0): PriceEvent =
    PriceEvent
      .newBuilder()
      .setSymbol(symbol)
      .setSecurityType(SecurityType.SECURITY_TYPE_EQUITY)
      .setLastPriceMicros(28040000L + row)
      .setSourceRow(row)
      .setTradeTime(
        Timestamp
          .newBuilder()
          .setSeconds(millis / 1000L)
          .setNanos((millis % 1000L).toInt * 1000000 + nanosExtra)
      )
      .build()

  private def runWindows(events: Seq[PriceEvent]): List[WindowClose] = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // Fail the test if the pipeline falls back to generic serialization.
    val config = new Configuration()
    config.set(PipelineOptions.GENERIC_TYPES, java.lang.Boolean.FALSE)
    env.configure(config)
    val input = env
      .fromData(events.asJava, new PriceEventTypeInformation)
      .assignTimestampsAndWatermarks(StreamingProcessor.priceEventWatermarks)
    val results = StreamingProcessor.closingPrices(input).executeAndCollect()
    try results.asScala.toList
    finally results.close()
  }

  test("real windows isolate symbols and put the exact boundary into the next window") {
    val beforeBoundary = trade("RUI.FR", start + fiveMinutes - 1, 14L)
    val atBoundary = trade("RUI.FR", start + fiveMinutes, 15L)
    val otherExchange = trade("RUI.ETR", start + fiveMinutes - 2, 16L)
    val results = runWindows(Seq(beforeBoundary, atBoundary, otherExchange))
    assertEquals(results.size, 3)
    val byWindow = results.map(window => (window.symbol, window.windowStartMillis) -> window).toMap
    assertEquals(
      byWindow(("RUI.FR", start)),
      WindowClose(beforeBoundary, start, start + fiveMinutes)
    )
    assertEquals(
      byWindow(("RUI.ETR", start)),
      WindowClose(otherExchange, start, start + fiveMinutes)
    )
    assertEquals(
      byWindow(("RUI.FR", start + fiveMinutes)),
      WindowClose(atBoundary, start + fiveMinutes, start + 2 * fiveMinutes)
    )
  }

  test("bounded replay closes its last window and out-of-order trades preserve nanosecond ties") {
    val selected = trade("RUI.FR", start + 100000L, 20L, nanosExtra = 2)
    val earlierNanos = trade("RUI.FR", start + 100000L, 99L, nanosExtra = 1)
    val lowerTie = trade("RUI.FR", start + 100000L, 19L, nanosExtra = 2)
    val older = trade("RUI.FR", start + 99999L, 100L)
    // The input ends within this window; there is no event from a later window.
    val results = runWindows(Seq(selected, earlierNanos, lowerTie, older))
    assertEquals(results, List(WindowClose(selected, start, start + fiveMinutes)))
  }
}
