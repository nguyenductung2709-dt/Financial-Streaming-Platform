package financial.streaming.latency

import com.google.protobuf.Timestamp
import financial.streaming.StreamingProcessor
import financial.streaming.serialization.PriceEventTypeInformation
import java.time.Instant
import market.v1.Events.{PriceEvent, SecurityType}
import market.v1.Latency.Stage
import org.apache.flink.configuration.{Configuration, PipelineOptions}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import scala.jdk.CollectionConverters._

class LatencyPipelineSuite extends munit.FunSuite {
  override val munitTimeout: scala.concurrent.duration.Duration =
    scala.concurrent.duration.Duration(60, "seconds")

  private val start = Instant.parse("2021-11-12T05:00:00Z").toEpochMilli
  private val fiveMinutes = 300000L

  private def trade(symbol: String, millis: Long, row: Long): PriceEvent =
    PriceEvent
      .newBuilder()
      .setSymbol(symbol)
      .setSecurityType(SecurityType.SECURITY_TYPE_EQUITY)
      .setLastPriceMicros(28040000L + row)
      .setSourceRow(row)
      .setTradeTime(
        Timestamp.newBuilder().setSeconds(millis / 1000L).setNanos((millis % 1000L).toInt * 1000000)
      )
      .build()

  // Two symbols, two windows each; the end of the bounded input fires every window.
  private val events = Seq(
    trade("RUI.FR", start + 10000L, 1L),
    trade("RUI.ETR", start + 20000L, 2L),
    trade("RUI.FR", start + fiveMinutes + 10000L, 3L),
    trade("RUI.ETR", start + fiveMinutes + 20000L, 4L)
  )

  private def input() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // Fail the test if the pipeline falls back to generic serialization.
    val config = new Configuration()
    config.set(PipelineOptions.GENERIC_TYPES, java.lang.Boolean.FALSE)
    env.configure(config)
    env
      .fromData(events.asJava, new PriceEventTypeInformation)
      .assignTimestampsAndWatermarks(StreamingProcessor.priceEventWatermarks)
  }

  private def stamps(latency: Boolean) = {
    val windows = StreamingProcessor.closingPrices(input(), latency)
    val emaResults = StreamingProcessor.emaValues(windows, latency)
    val advisories = StreamingProcessor.crossoverResults(emaResults, latency)
    val results =
      StreamingProcessor.latencyStamps(windows, emaResults, advisories).executeAndCollect()
    try results.asScala.toList
    finally results.close()
  }

  test("three stamps per fired window cover every stage") {
    val collected = stamps(latency = true)
    assertEquals(collected.size, 12)
    val stagesByWindow = collected
      .groupBy(stamp => (stamp.symbol, stamp.windowEndMillis))
      .map { case (window, group) => window -> group.map(_.stage).toSet }
    assertEquals(
      stagesByWindow.keySet,
      Set(
        ("RUI.FR", start + fiveMinutes),
        ("RUI.FR", start + 2 * fiveMinutes),
        ("RUI.ETR", start + fiveMinutes),
        ("RUI.ETR", start + 2 * fiveMinutes)
      )
    )
    stagesByWindow.values.foreach(stages =>
      assertEquals(
        stages,
        Set(Stage.STAGE_WINDOW_FIRED, Stage.STAGE_EMA_COMPUTED, Stage.STAGE_CROSSOVER_EVALUATED)
      )
    )
  }

  test("with latency off the main outputs are identical and no stamps appear") {
    def windows(latency: Boolean) = {
      val results = StreamingProcessor.closingPrices(input(), latency).executeAndCollect()
      try results.asScala.toSet
      finally results.close()
    }
    assertEquals(windows(latency = false), windows(latency = true))
    assertEquals(stamps(latency = false), Nil)
  }
}
