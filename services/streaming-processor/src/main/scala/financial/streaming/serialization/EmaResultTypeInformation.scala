package financial.streaming.serialization

import financial.streaming.model.EmaResult
import org.apache.flink.api.common.serialization.SerializerConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer

final class EmaResultTypeInformation extends TypeInformation[EmaResult] {
  override def isBasicType: Boolean = false
  override def isTupleType: Boolean = false
  override def getArity: Int = 1
  override def getTotalFields: Int = 1
  override def getTypeClass: Class[EmaResult] = classOf[EmaResult]
  override def isKeyType: Boolean = false

  override def createSerializer(config: SerializerConfig): TypeSerializer[EmaResult] =
    new EmaResultSerializer

  override def canEqual(other: Any): Boolean = other.isInstanceOf[EmaResultTypeInformation]
  override def equals(other: Any): Boolean = canEqual(other)
  override def hashCode(): Int = classOf[EmaResultTypeInformation].hashCode()
  override def toString: String = "EmaResult(decimal, Protobuf)"
}
