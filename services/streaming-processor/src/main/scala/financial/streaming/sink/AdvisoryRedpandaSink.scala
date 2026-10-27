package financial.streaming.sink

import financial.streaming.model.CrossoverResult
import java.nio.charset.StandardCharsets
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.kafka.clients.producer.ProducerRecord

object AdvisoryRedpandaSink {
  def create(brokers: String, topic: String): KafkaSink[CrossoverResult] =
    KafkaSink
      .builder[CrossoverResult]()
      .setBootstrapServers(brokers)
      .setRecordSerializer(new AdvisoryRecordSerializationSchema(topic))
      .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build()
}

final class AdvisoryRecordSerializationSchema(topic: String)
    extends KafkaRecordSerializationSchema[CrossoverResult] {
  override def serialize(
      result: CrossoverResult,
      context: KafkaRecordSerializationSchema.KafkaSinkContext,
      timestamp: java.lang.Long
  ): ProducerRecord[Array[Byte], Array[Byte]] = {
    val message = TradeAdvisoryMapper.toProto(result)
    new ProducerRecord[Array[Byte], Array[Byte]](
      topic,
      null,
      timestamp,
      message.getEmaWindow.getSymbol.getBytes(StandardCharsets.UTF_8),
      message.toByteArray
    )
  }
}
