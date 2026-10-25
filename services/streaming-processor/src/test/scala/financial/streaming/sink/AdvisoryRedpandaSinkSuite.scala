package financial.streaming.sink

import financial.streaming.model.{CrossoverResult, EmaResult, WindowClose}
import java.math.BigDecimal
import java.nio.charset.StandardCharsets
import market.v1.Advisory.{AdvisoryAction, TradeAdvisory}
import market.v1.Events.PriceEvent
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.KafkaSinkContext
import org.apache.flink.util.InstantiationUtil

class AdvisoryRedpandaSinkSuite extends munit.FunSuite {
  private val context = new KafkaSinkContext {
    override def getParallelInstanceId: Int = 0
    override def getNumberOfParallelInstances: Int = 1
    override def getPartitionsForTopic(topic: String): Array[Int] = Array(0)
  }

  private def result(symbol: String = "IUSAC.FR", price: Long = 418210000L): CrossoverResult =
    CrossoverResult(
      EmaResult(
        WindowClose(
          PriceEvent.newBuilder().setSymbol(symbol).setLastPriceMicros(price).build(),
          1636693200000L,
          1636693500000L
        ),
        new BigDecimal("21.44666666666666666666666666666667"),
        new BigDecimal("8.281386138613861386138613861386139")
      ),
      AdvisoryAction.ADVISORY_ACTION_BUY,
      new BigDecimal("7.000000000000000000000000000000001"),
      new BigDecimal("8.00")
    )

  test("publish raw BUY and SELL Protobuf advisories keyed by symbol to the configured topic") {
    Seq(AdvisoryAction.ADVISORY_ACTION_BUY, AdvisoryAction.ADVISORY_ACTION_SELL).foreach { action =>
      val input = result().copy(action = action)
      val timestamp = java.lang.Long.valueOf(1636693499999L)
      val record = new AdvisoryRecordSerializationSchema("advisories-custom")
        .serialize(input, context, timestamp)
      assertEquals(record.topic(), "advisories-custom")
      assertEquals(new String(record.key(), StandardCharsets.UTF_8), "IUSAC.FR")
      assertEquals(record.timestamp(), timestamp)
      assertEquals(record.partition(), null)
      assertEquals(TradeAdvisory.parseFrom(record.value()), TradeAdvisoryMapper.toProto(input))
    }
  }

  test("UTF-8 keys, unsigned prices and previous EMA decimals retain their complete values") {
    val record = new AdvisoryRecordSerializationSchema("trade-advisories")
      .serialize(result("ÜNICODE.FR", -1L), context, null)
    assertEquals(new String(record.key(), StandardCharsets.UTF_8), "ÜNICODE.FR")
    assertEquals(record.timestamp(), null)
    val message = TradeAdvisory.parseFrom(record.value())
    assertEquals(
      java.lang.Long.toUnsignedString(message.getEmaWindow.getClosingPriceMicros),
      "18446744073709551615"
    )
    assertEquals(message.getPreviousEma38, "7.000000000000000000000000000000001")
    assertEquals(message.getPreviousEma100, "8.00")
  }

  test("record serializer can be shipped to Flink workers with its topic configuration") {
    val schema = new AdvisoryRecordSerializationSchema("trade-advisories")
    val restored = InstantiationUtil.deserializeObject[AdvisoryRecordSerializationSchema](
      InstantiationUtil.serializeObject(schema),
      getClass.getClassLoader
    )
    val record = restored.serialize(result(), context, null)
    assertEquals(record.topic(), "trade-advisories")
    assertEquals(TradeAdvisory.parseFrom(record.value()), TradeAdvisoryMapper.toProto(result()))
  }
}
