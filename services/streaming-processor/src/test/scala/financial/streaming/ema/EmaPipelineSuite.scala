package financial.streaming.ema

import com.google.protobuf.Timestamp
import financial.streaming.StreamingProcessor
import financial.streaming.serialization.PriceEventTypeInformation
import financial.streaming.sink.EmaWindowMapper
import market.v1.Events.{PriceEvent, SecurityType}
import org.apache.flink.configuration.{Configuration, PipelineOptions}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import scala.jdk.CollectionConverters._

class EmaPipelineSuite extends munit.FunSuite {
  override val munitTimeout: scala.concurrent.duration.Duration =
    scala.concurrent.duration.Duration(60, "seconds")

  private def trade(symbol: String, millis: Long, price: Long, row: Long): PriceEvent =
    PriceEvent
      .newBuilder()
      .setSymbol(symbol)
      .setSecurityType(SecurityType.SECURITY_TYPE_EQUITY)
      .setLastPriceMicros(price)
      .setSourceRow(row)
      .setTradeTime(
        Timestamp
          .newBuilder()
          .setSeconds(millis / 1000L)
          .setNanos((millis % 1000L).toInt * 1000000)
      )
      .build()

  test(
    "production window and EMA operators preserve independent histories with generic serialization disabled"
  ) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    val config = new Configuration()
    config.set(PipelineOptions.GENERIC_TYPES, java.lang.Boolean.FALSE)
    env.configure(config)
    val first = trade("RUI.FR", 2000L, 39000000L, 14L)
    val second = trade("RUI.FR", 300000L, 78000000L, 15L)
    val other = trade("RUI.ETR", 1000L, 78000000L, 16L)
    val older = trade("RUI.FR", 1000L, 99000000L, 17L)
    val input = env
      .fromData(List(first, second, other, older).asJava, new PriceEventTypeInformation)
      .assignTimestampsAndWatermarks(StreamingProcessor.priceEventWatermarks)
    val iterator =
      StreamingProcessor.emaValues(StreamingProcessor.closingPrices(input)).executeAndCollect()
    val results =
      try iterator.asScala.toList.map(EmaWindowMapper.toProto)
      finally iterator.close()
    assertEquals(results.size, 3)
    val byWindow = results.map(r => (r.getSymbol, r.getWindowStart.getSeconds) -> r).toMap
    val initialA = byWindow(("RUI.FR", 0L))
    val nextA = byWindow(("RUI.FR", 300L))
    val initialB = byWindow(("RUI.ETR", 0L))
    assertEquals(initialA.getClosingPriceMicros, 39000000L)
    assertEquals(initialA.getWindowEnd.getSeconds, 300L)
    assertEquals(initialA.getEma38, "2.000000")
    assertEquals(initialA.getEma100, "0.7722772277227722772277227722772277")
    assertEquals(initialB.getEma38, "4.000000")
    assertEquals(nextA.getClosingPriceMicros, 78000000L)
    assertEquals(nextA.getEma38, "5.897435897435897435897435897435897")
    assertEquals(nextA.getEma100, "2.301539064797568865797470836192530")
  }
}
