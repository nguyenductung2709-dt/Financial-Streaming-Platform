package financial.streaming.sink

import financial.streaming.model.{EmaResult, WindowClose}
import java.math.BigDecimal
import java.nio.charset.StandardCharsets
import market.v1.Ema.EmaWindow
import market.v1.Events.PriceEvent
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.KafkaSinkContext
import org.apache.flink.util.InstantiationUtil

class EmaRedpandaSinkSuite extends munit.FunSuite {
  private val context = new KafkaSinkContext {
    override def getParallelInstanceId: Int = 0
    override def getNumberOfParallelInstances: Int = 1
    override def getPartitionsForTopic(topic: String): Array[Int] = Array(0)
  }

  private def result(symbol: String = "IUSAC.FR", price: Long = 418210000L): EmaResult =
    EmaResult(
      WindowClose(
        PriceEvent.newBuilder().setSymbol(symbol).setLastPriceMicros(price).build(),
        1636693200000L,
        1636693500000L
      ),
      new BigDecimal("21.44666666666666666666666666666667"),
      new BigDecimal("8.281386138613861386138613861386139")
    )

  test("publish a raw Protobuf EmaWindow keyed by symbol to the configured output topic") {
    val input = result()
    val timestamp = java.lang.Long.valueOf(1636693499999L)
    val record = new EmaRecordSerializationSchema("ema-custom").serialize(input, context, timestamp)
    assertEquals(record.topic(), "ema-custom")
    assertEquals(new String(record.key(), StandardCharsets.UTF_8), "IUSAC.FR")
    assertEquals(record.timestamp(), timestamp)
    assertEquals(record.partition(), null)
    assertEquals(EmaWindow.parseFrom(record.value()), EmaWindowMapper.toProto(input))
  }

  test("UTF-8 keys and unsigned prices retain their complete values") {
    val record = new EmaRecordSerializationSchema("ema-windows")
      .serialize(result("ÜNICODE.FR", -1L), context, null)
    assertEquals(new String(record.key(), StandardCharsets.UTF_8), "ÜNICODE.FR")
    assertEquals(record.timestamp(), null)
    val message = EmaWindow.parseFrom(record.value())
    assertEquals(
      java.lang.Long.toUnsignedString(message.getClosingPriceMicros),
      "18446744073709551615"
    )
    assertEquals(message.getEma38, "21.44666666666666666666666666666667")
    assertEquals(message.getEma100, "8.281386138613861386138613861386139")
  }

  test("record serializer can be shipped to Flink workers with its topic configuration") {
    val schema = new EmaRecordSerializationSchema("ema-windows")
    val restored = InstantiationUtil.deserializeObject[EmaRecordSerializationSchema](
      InstantiationUtil.serializeObject(schema),
      getClass.getClassLoader
    )
    val record = restored.serialize(result(), context, null)
    assertEquals(record.topic(), "ema-windows")
    assertEquals(EmaWindow.parseFrom(record.value()), EmaWindowMapper.toProto(result()))
  }
}
