package financial.streaming.sink

import financial.streaming.model.StageStamp
import java.nio.charset.StandardCharsets
import market.v1.Latency
import market.v1.Latency.Stage
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.KafkaSinkContext
import org.apache.flink.util.InstantiationUtil

class LatencyRedpandaSinkSuite extends munit.FunSuite {
  private val context = new KafkaSinkContext {
    override def getParallelInstanceId: Int = 0
    override def getNumberOfParallelInstances: Int = 1
    override def getPartitionsForTopic(topic: String): Array[Int] = Array(0)
  }

  private def stamp(symbol: String = "IUSAC.FR"): StageStamp =
    StageStamp(symbol, 1636693500123L, Stage.STAGE_CROSSOVER_EVALUATED, 1636693501250L, true)

  test("publish a raw Protobuf StageStamp keyed by symbol to the configured topic") {
    val input = stamp()
    val record =
      new LatencyRecordSerializationSchema("latency-custom").serialize(input, context, null)
    assertEquals(record.topic(), "latency-custom")
    assertEquals(new String(record.key(), StandardCharsets.UTF_8), "IUSAC.FR")
    assertEquals(record.partition(), null)
    val message = Latency.StageStamp.parseFrom(record.value())
    assertEquals(message, StageStampMapper.toProto(input))
    assertEquals(message.getSymbol, "IUSAC.FR")
    assertEquals(message.getStage, Stage.STAGE_CROSSOVER_EVALUATED)
    assert(message.getAdvisoryEmitted)
    assertEquals(message.getWindowEnd.getSeconds, 1636693500L)
    assertEquals(message.getWindowEnd.getNanos, 123000000)
    assertEquals(message.getAt.getSeconds, 1636693501L)
    assertEquals(message.getAt.getNanos, 250000000)
  }

  test("UTF-8 keys retain their complete value") {
    val record = new LatencyRecordSerializationSchema("latency-stamps")
      .serialize(stamp("ÜNICODE.FR"), context, null)
    assertEquals(new String(record.key(), StandardCharsets.UTF_8), "ÜNICODE.FR")
  }

  test("record serializer can be shipped to Flink workers with its topic configuration") {
    val schema = new LatencyRecordSerializationSchema("latency-stamps")
    val restored = InstantiationUtil.deserializeObject[LatencyRecordSerializationSchema](
      InstantiationUtil.serializeObject(schema),
      getClass.getClassLoader
    )
    val record = restored.serialize(stamp(), context, null)
    assertEquals(record.topic(), "latency-stamps")
    assertEquals(Latency.StageStamp.parseFrom(record.value()), StageStampMapper.toProto(stamp()))
  }
}
