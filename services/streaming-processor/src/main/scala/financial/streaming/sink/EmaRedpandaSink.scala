package financial.streaming.sink

import financial.streaming.model.EmaResult
import java.nio.charset.StandardCharsets
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.kafka.clients.producer.ProducerRecord

object EmaRedpandaSink {
  def create(brokers: String, topic: String): KafkaSink[EmaResult] =
    KafkaSink
      .builder[EmaResult]()
      .setBootstrapServers(brokers)
      .setRecordSerializer(new EmaRecordSerializationSchema(topic))
      .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build()
}

final class EmaRecordSerializationSchema(topic: String)
    extends KafkaRecordSerializationSchema[EmaResult] {
  override def serialize(
      result: EmaResult,
      context: KafkaRecordSerializationSchema.KafkaSinkContext,
      timestamp: java.lang.Long
  ): ProducerRecord[Array[Byte], Array[Byte]] = {
    val message = EmaWindowMapper.toProto(result)
    new ProducerRecord[Array[Byte], Array[Byte]](
      topic,
      null,
      timestamp,
      message.getSymbol.getBytes(StandardCharsets.UTF_8),
      message.toByteArray
    )
  }
}
