package financial.streaming.serialization

import financial.streaming.model.CrossoverResult
import org.apache.flink.api.common.serialization.SerializerConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer

final class CrossoverResultTypeInformation extends TypeInformation[CrossoverResult] {
  override def isBasicType: Boolean = false
  override def isTupleType: Boolean = false
  override def getArity: Int = 1
  override def getTotalFields: Int = 1
  override def getTypeClass: Class[CrossoverResult] = classOf[CrossoverResult]
  override def isKeyType: Boolean = false

  override def createSerializer(config: SerializerConfig): TypeSerializer[CrossoverResult] =
    new CrossoverResultSerializer

  override def canEqual(other: Any): Boolean = other.isInstanceOf[CrossoverResultTypeInformation]
  override def equals(other: Any): Boolean = canEqual(other)
  override def hashCode(): Int = classOf[CrossoverResultTypeInformation].hashCode()
  override def toString: String = "CrossoverResult(decimal, Protobuf)"
}
