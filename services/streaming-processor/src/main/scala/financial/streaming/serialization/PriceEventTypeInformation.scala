package financial.streaming.serialization

import financial.streaming.source.PriceEventDecoder

import java.io.IOException
import market.v1.Events.PriceEvent
import org.apache.flink.api.common.serialization.SerializerConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.{
  SimpleTypeSerializerSnapshot,
  TypeSerializer,
  TypeSerializerSnapshot
}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

// An explicit serializer avoids reflective Kryo serialization of generated Protobuf classes.
final class PriceEventTypeInformation extends TypeInformation[PriceEvent] {
  override def isBasicType: Boolean = false
  override def isTupleType: Boolean = false
  override def getArity: Int = 1
  override def getTotalFields: Int = 1
  override def getTypeClass: Class[PriceEvent] = classOf[PriceEvent]
  override def isKeyType: Boolean = false
  override def createSerializer(config: SerializerConfig): TypeSerializer[PriceEvent] =
    new PriceEventSerializer
  override def canEqual(other: Any): Boolean = other.isInstanceOf[PriceEventTypeInformation]
  override def equals(other: Any): Boolean = canEqual(other)
  override def hashCode(): Int = classOf[PriceEventTypeInformation].hashCode()
  override def toString: String = "PriceEvent(Protobuf)"
}

final class PriceEventSerializer extends TypeSerializer[PriceEvent] {
  override def isImmutableType: Boolean = true
  override def duplicate(): TypeSerializer[PriceEvent] = this
  override def createInstance(): PriceEvent = PriceEvent.getDefaultInstance
  override def copy(value: PriceEvent): PriceEvent = value
  override def copy(value: PriceEvent, reuse: PriceEvent): PriceEvent = value
  override def getLength: Int = -1
  override def serialize(value: PriceEvent, target: DataOutputView): Unit = {
    val bytes = value.toByteArray
    require(
      bytes.length <= PriceEventDecoder.MaxMessageBytes,
      "PriceEvent exceeds maximum message size"
    )
    target.writeInt(bytes.length)
    target.write(bytes)
  }
  override def deserialize(source: DataInputView): PriceEvent = {
    val length = source.readInt()
    if (length < 0 || length > PriceEventDecoder.MaxMessageBytes)
      throw new IOException(s"Invalid serialized PriceEvent length: $length")
    val bytes = new Array[Byte](length)
    source.readFully(bytes)
    PriceEvent.parseFrom(bytes)
  }
  override def deserialize(reuse: PriceEvent, source: DataInputView): PriceEvent = deserialize(
    source
  )
  override def copy(source: DataInputView, target: DataOutputView): Unit =
    serialize(deserialize(source), target)
  override def equals(other: Any): Boolean = other.isInstanceOf[PriceEventSerializer]
  override def hashCode(): Int = classOf[PriceEventSerializer].hashCode()
  override def snapshotConfiguration(): TypeSerializerSnapshot[PriceEvent] =
    new PriceEventSerializerSnapshot
}

// Public no-argument constructor is required when Flink restores serializer metadata.
final class PriceEventSerializerSnapshot
    extends SimpleTypeSerializerSnapshot[PriceEvent](() => new PriceEventSerializer)
