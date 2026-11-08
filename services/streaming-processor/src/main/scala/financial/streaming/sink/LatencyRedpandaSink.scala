package financial.streaming.sink

import financial.streaming.model.StageStamp
import java.nio.charset.StandardCharsets
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.kafka.clients.producer.ProducerRecord

object LatencyRedpandaSink {
  def create(brokers: String, topic: String): KafkaSink[StageStamp] =
    KafkaSink
      .builder[StageStamp]()
      .setBootstrapServers(brokers)
      .setRecordSerializer(new LatencyRecordSerializationSchema(topic))
      .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build()
}

final class LatencyRecordSerializationSchema(topic: String)
    extends KafkaRecordSerializationSchema[StageStamp] {
  // The stream's event time is meaningless for a measurement record; let the producer stamp it.
  private val noTimestamp: java.lang.Long = null

  override def serialize(
      stamp: StageStamp,
      context: KafkaRecordSerializationSchema.KafkaSinkContext,
      timestamp: java.lang.Long
  ): ProducerRecord[Array[Byte], Array[Byte]] =
    new ProducerRecord[Array[Byte], Array[Byte]](
      topic,
      null,
      noTimestamp,
      stamp.symbol.getBytes(StandardCharsets.UTF_8),
      StageStampMapper.toProto(stamp).toByteArray
    )
}
