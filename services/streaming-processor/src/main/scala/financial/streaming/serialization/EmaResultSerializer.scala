package financial.streaming.serialization

import financial.streaming.model.EmaResult
import java.math.BigDecimal
import org.apache.flink.api.common.typeutils.{
  SimpleTypeSerializerSnapshot,
  TypeSerializer,
  TypeSerializerSnapshot
}
import org.apache.flink.api.common.typeutils.base.BigDecSerializer
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

final class EmaResultSerializer extends TypeSerializer[EmaResult] {
  private val windows = new WindowCloseSerializer
  private val decimals = BigDecSerializer.INSTANCE

  override def isImmutableType: Boolean = true
  override def duplicate(): TypeSerializer[EmaResult] = this
  override def createInstance(): EmaResult =
    EmaResult(windows.createInstance(), BigDecimal.ZERO, BigDecimal.ZERO)
  override def copy(value: EmaResult): EmaResult = value
  override def copy(value: EmaResult, reuse: EmaResult): EmaResult = value
  override def getLength: Int = -1

  override def serialize(value: EmaResult, target: DataOutputView): Unit = {
    windows.serialize(value.window, target)
    decimals.serialize(value.ema38, target)
    decimals.serialize(value.ema100, target)
  }
  override def deserialize(source: DataInputView): EmaResult =
    EmaResult(
      windows.deserialize(source),
      decimals.deserialize(source),
      decimals.deserialize(source)
    )
  override def deserialize(reuse: EmaResult, source: DataInputView): EmaResult = deserialize(source)
  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    windows.copy(source, target)
    decimals.copy(source, target)
    decimals.copy(source, target)
  }
  override def equals(other: Any): Boolean = other.isInstanceOf[EmaResultSerializer]
  override def hashCode(): Int = classOf[EmaResultSerializer].hashCode()
  override def snapshotConfiguration(): TypeSerializerSnapshot[EmaResult] =
    new EmaResultSerializerSnapshot
}

final class EmaResultSerializerSnapshot
    extends SimpleTypeSerializerSnapshot[EmaResult](() => new EmaResultSerializer)
