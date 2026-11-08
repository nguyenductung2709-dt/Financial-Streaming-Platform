package financial.streaming.serialization

import financial.streaming.model.StageStamp
import org.apache.flink.api.common.serialization.SerializerConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer

final class StageStampTypeInformation extends TypeInformation[StageStamp] {
  override def isBasicType: Boolean = false
  override def isTupleType: Boolean = false
  override def getArity: Int = 1
  override def getTotalFields: Int = 1
  override def getTypeClass: Class[StageStamp] = classOf[StageStamp]
  override def isKeyType: Boolean = false

  override def createSerializer(config: SerializerConfig): TypeSerializer[StageStamp] =
    new StageStampSerializer

  override def canEqual(other: Any): Boolean = other.isInstanceOf[StageStampTypeInformation]
  override def equals(other: Any): Boolean = canEqual(other)
  override def hashCode(): Int = classOf[StageStampTypeInformation].hashCode()
  override def toString: String = "StageStamp(latency)"
}
