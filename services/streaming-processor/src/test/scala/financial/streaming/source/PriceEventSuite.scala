package financial.streaming.source

import financial.streaming.serialization.PriceEventSerializer
import financial.streaming.serialization.PriceEventTypeInformation

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException}
import com.google.protobuf.Timestamp
import market.v1.Events.{PriceEvent, SecurityType}
import org.apache.flink.core.memory.{DataInputViewStreamWrapper, DataOutputViewStreamWrapper}
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord

class PriceEventSuite extends munit.FunSuite {
  def valid: PriceEvent = PriceEvent
    .newBuilder()
    .setSymbol("RUI.FR")
    .setSecurityType(SecurityType.SECURITY_TYPE_EQUITY)
    .setLastPriceMicros(28040000L)
    .setSourceRow(728610L)
    .setTradeTime(Timestamp.newBuilder().setSeconds(1636700400L).setNanos(179000000))
    .build()

  test("decode exact producer wire format without float conversion") {
    assertEquals(PriceEventDecoder.decode(valid.toByteArray), valid)
  }
  test("uint64 fields preserve their unsigned bit patterns") {
    val large = valid.toBuilder.setLastPriceMicros(-1L).setSourceRow(-1L).build()
    assertEquals(PriceEventDecoder.decode(large.toByteArray), large)
  }
  test("reject tombstones, empty, oversized and malformed payloads") {
    Seq(null, Array.emptyByteArray, Array.fill[Byte](PriceEventDecoder.MaxMessageBytes + 1)(0))
      .foreach(b => intercept[IllegalArgumentException](PriceEventDecoder.decode(b)))
    intercept[com.google.protobuf.InvalidProtocolBufferException] {
      PriceEventDecoder.decode(Array[Byte](10, 127))
    }
  }
  test("reject missing or invalid required business fields") {
    Seq(
      valid.toBuilder.clearSymbol().build(),
      valid.toBuilder.setSymbol("\n").build(),
      valid.toBuilder.setSecurityTypeValue(99).build(),
      valid.toBuilder.setLastPriceMicros(0L).build(),
      valid.toBuilder.clearSourceRow().build(),
      valid.toBuilder.clearTradeTime().build(),
      valid.toBuilder.setTradeTime(Timestamp.newBuilder().setSeconds(253402300800L)).build(),
      valid.toBuilder.setTradeTime(Timestamp.newBuilder().setNanos(-1)).build()
    )
      .foreach(e => intercept[IllegalArgumentException](PriceEventDecoder.decode(e.toByteArray)))
  }
  test("Kafka decoding errors include source coordinates and retain their cause") {
    val record = new ConsumerRecord[Array[Byte], Array[Byte]](
      "trade-events",
      2,
      73L,
      null,
      Array.emptyByteArray
    )
    val schema = new PriceEventDeserializationSchema
    var collected = false
    val out = new Collector[PriceEvent] {
      override def collect(e: PriceEvent): Unit = { collected = true }
      override def close(): Unit = ()
    }
    val error = intercept[IOException](schema.deserialize(record, out))
    assert(error.getMessage.contains("topic=trade-events partition=2 offset=73"))
    assert(error.getCause != null)
    assert(!collected)
  }
  test("Kafka deserializer emits a typed PriceEvent") {
    val record =
      new ConsumerRecord[Array[Byte], Array[Byte]]("trade-events", 0, 1L, null, valid.toByteArray)
    var decoded: PriceEvent = null
    val schema = new PriceEventDeserializationSchema
    schema.deserialize(
      record,
      new Collector[PriceEvent] {
        override def collect(e: PriceEvent): Unit = { decoded = e }
        override def close(): Unit = ()
      }
    )
    assertEquals(decoded, valid)
    assert(schema.getProducedType.isInstanceOf[PriceEventTypeInformation])
  }
  test("Flink serializer and restored serializer preserve complete Protobuf events") {
    val serializer = new PriceEventSerializer
    val bytes = new ByteArrayOutputStream
    val large = valid.toBuilder.setLastPriceMicros(-1L).build()
    serializer.serialize(large, new DataOutputViewStreamWrapper(bytes))
    val restored = serializer.snapshotConfiguration().restoreSerializer()
    val decoded = restored.deserialize(
      new DataInputViewStreamWrapper(new ByteArrayInputStream(bytes.toByteArray))
    )
    assertEquals(decoded, large)
    assert(serializer.copy(large) eq large)
  }
  test("Flink serializer rejects corrupt length prefixes") {
    Seq(-1, PriceEventDecoder.MaxMessageBytes + 1).foreach { size =>
      val bytes = new ByteArrayOutputStream
      new DataOutputViewStreamWrapper(bytes).writeInt(size)
      intercept[IOException] {
        new PriceEventSerializer().deserialize(
          new DataInputViewStreamWrapper(new ByteArrayInputStream(bytes.toByteArray))
        )
      }
    }
  }
}
