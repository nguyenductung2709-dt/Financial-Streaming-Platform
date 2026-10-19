package financial.streaming

import java.io.IOException
import market.v1.Events.PriceEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.util.Collector
import scala.util.control.NonFatal

// Kafka adapter: add record coordinates to decoding errors and emit valid events.
final class PriceEventDeserializationSchema extends KafkaRecordDeserializationSchema[PriceEvent] {
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]], out: Collector[PriceEvent]): Unit = {
    val event = try PriceEventDecoder.decode(record.value()) catch {
      case NonFatal(e) => throw new IOException(
        s"Invalid PriceEvent at topic=${record.topic()} partition=${record.partition()} offset=${record.offset()}: ${e.getMessage}", e)
    }
    out.collect(event)
  }
  override def getProducedType: TypeInformation[PriceEvent] = new PriceEventTypeInformation
}
