package financial.streaming.serialization

import financial.streaming.model.StageStamp
import market.v1.Latency.Stage
import org.apache.flink.api.common.typeutils.{
  SimpleTypeSerializerSnapshot,
  TypeSerializer,
  TypeSerializerSnapshot
}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

/** Internal format: symbol (UTF), window end millis, stage number, stamp millis, advisory flag. */
final class StageStampSerializer extends TypeSerializer[StageStamp] {
  override def isImmutableType: Boolean = true
  override def duplicate(): TypeSerializer[StageStamp] = this
  override def createInstance(): StageStamp =
    StageStamp("", 0L, Stage.STAGE_UNSPECIFIED, 0L, advisoryEmitted = false)
  override def copy(value: StageStamp): StageStamp = value
  override def copy(value: StageStamp, reuse: StageStamp): StageStamp = value
  override def getLength: Int = -1

  override def serialize(value: StageStamp, target: DataOutputView): Unit = {
    target.writeUTF(value.symbol)
    target.writeLong(value.windowEndMillis)
    target.writeInt(value.stage.getNumber)
    target.writeLong(value.atMillis)
    target.writeBoolean(value.advisoryEmitted)
  }

  override def deserialize(source: DataInputView): StageStamp = {
    val symbol = source.readUTF()
    val windowEnd = source.readLong()
    val stageNumber = source.readInt()
    val stage = Stage.forNumber(stageNumber)
    require(stage != null, s"unknown stage number: $stageNumber")
    val at = source.readLong()
    StageStamp(symbol, windowEnd, stage, at, source.readBoolean())
  }

  override def deserialize(reuse: StageStamp, source: DataInputView): StageStamp =
    deserialize(source)

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    target.writeUTF(source.readUTF())
    target.writeLong(source.readLong())
    target.writeInt(source.readInt())
    target.writeLong(source.readLong())
    target.writeBoolean(source.readBoolean())
  }

  override def equals(other: Any): Boolean = other.isInstanceOf[StageStampSerializer]
  override def hashCode(): Int = classOf[StageStampSerializer].hashCode()
  override def snapshotConfiguration(): TypeSerializerSnapshot[StageStamp] =
    new StageStampSerializerSnapshot
}

// Flink needs a public no-argument constructor to restore serializer metadata.
final class StageStampSerializerSnapshot
    extends SimpleTypeSerializerSnapshot[StageStamp](() => new StageStampSerializer)
