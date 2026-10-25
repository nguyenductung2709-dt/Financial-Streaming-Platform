package financial.streaming.serialization

import financial.streaming.model.CrossoverResult
import java.io.IOException
import java.math.BigDecimal
import market.v1.Advisory.AdvisoryAction
import org.apache.flink.api.common.typeutils.{
  SimpleTypeSerializerSnapshot,
  TypeSerializer,
  TypeSerializerSnapshot
}
import org.apache.flink.api.common.typeutils.base.BigDecSerializer
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

final class CrossoverResultSerializer extends TypeSerializer[CrossoverResult] {
  private val emas = new EmaResultSerializer
  private val decimals = BigDecSerializer.INSTANCE

  override def isImmutableType: Boolean = true
  override def duplicate(): TypeSerializer[CrossoverResult] = this
  override def createInstance(): CrossoverResult =
    CrossoverResult(
      emas.createInstance(),
      AdvisoryAction.ADVISORY_ACTION_UNSPECIFIED,
      BigDecimal.ZERO,
      BigDecimal.ZERO
    )
  override def copy(value: CrossoverResult): CrossoverResult = value
  override def copy(value: CrossoverResult, reuse: CrossoverResult): CrossoverResult = value
  override def getLength: Int = -1

  override def serialize(value: CrossoverResult, target: DataOutputView): Unit = {
    emas.serialize(value.ema, target)
    target.writeInt(value.action.getNumber)
    decimals.serialize(value.previousEma38, target)
    decimals.serialize(value.previousEma100, target)
  }

  override def deserialize(source: DataInputView): CrossoverResult = {
    val ema = emas.deserialize(source)
    val actionNumber = source.readInt()
    val action = Option(AdvisoryAction.forNumber(actionNumber)).getOrElse(
      throw new IOException(s"Unknown advisory action: $actionNumber")
    )
    CrossoverResult(ema, action, decimals.deserialize(source), decimals.deserialize(source))
  }

  override def deserialize(reuse: CrossoverResult, source: DataInputView): CrossoverResult =
    deserialize(source)

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    emas.copy(source, target)
    target.writeInt(source.readInt())
    decimals.copy(source, target)
    decimals.copy(source, target)
  }

  override def equals(other: Any): Boolean = other.isInstanceOf[CrossoverResultSerializer]
  override def hashCode(): Int = classOf[CrossoverResultSerializer].hashCode()
  override def snapshotConfiguration(): TypeSerializerSnapshot[CrossoverResult] =
    new CrossoverResultSerializerSnapshot
}

final class CrossoverResultSerializerSnapshot
    extends SimpleTypeSerializerSnapshot[CrossoverResult](() => new CrossoverResultSerializer)
