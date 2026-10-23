package financial.streaming

import financial.streaming.model.WindowClose
import market.v1.Events.PriceEvent
import org.apache.flink.api.common.typeutils.{
  SimpleTypeSerializerSnapshot,
  TypeSerializer,
  TypeSerializerSnapshot
}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

/** Internal format: window start millis, window end millis, then a length-prefixed PriceEvent. */
final class WindowCloseSerializer extends TypeSerializer[WindowClose] {
  private val tradeSerializer = new PriceEventSerializer

  override def isImmutableType: Boolean = true
  override def duplicate(): TypeSerializer[WindowClose] = this

  override def createInstance(): WindowClose =
    WindowClose(PriceEvent.getDefaultInstance, 0L, 0L)

  override def copy(value: WindowClose): WindowClose = value
  override def copy(value: WindowClose, reuse: WindowClose): WindowClose = value
  override def getLength: Int = -1

  override def serialize(value: WindowClose, target: DataOutputView): Unit = {
    target.writeLong(value.windowStartMillis)
    target.writeLong(value.windowEndMillis)
    tradeSerializer.serialize(value.closingTrade, target)
  }

  override def deserialize(source: DataInputView): WindowClose = {
    val start = source.readLong()
    val end = source.readLong()
    val trade = tradeSerializer.deserialize(source)
    WindowClose(trade, start, end)
  }

  override def deserialize(reuse: WindowClose, source: DataInputView): WindowClose =
    deserialize(source)

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    target.writeLong(source.readLong())
    target.writeLong(source.readLong())
    tradeSerializer.copy(source, target)
  }

  override def equals(other: Any): Boolean = other.isInstanceOf[WindowCloseSerializer]
  override def hashCode(): Int = classOf[WindowCloseSerializer].hashCode()

  override def snapshotConfiguration(): TypeSerializerSnapshot[WindowClose] =
    new WindowCloseSerializerSnapshot
}

// Flink needs a public no-argument constructor to restore serializer metadata.
final class WindowCloseSerializerSnapshot
    extends SimpleTypeSerializerSnapshot[WindowClose](() => new WindowCloseSerializer)
