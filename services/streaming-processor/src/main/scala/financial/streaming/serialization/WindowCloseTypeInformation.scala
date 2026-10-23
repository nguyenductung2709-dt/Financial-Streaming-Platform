package financial.streaming.serialization

import financial.streaming.model.WindowClose
import org.apache.flink.api.common.serialization.SerializerConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer

/** Treat window results as opaque values serialized with our explicit Protobuf-based format. */
final class WindowCloseTypeInformation extends TypeInformation[WindowClose] {
  override def isBasicType: Boolean = false
  override def isTupleType: Boolean = false
  override def getArity: Int = 1
  override def getTotalFields: Int = 1
  override def getTypeClass: Class[WindowClose] = classOf[WindowClose]
  override def isKeyType: Boolean = false

  override def createSerializer(config: SerializerConfig): TypeSerializer[WindowClose] =
    new WindowCloseSerializer

  override def canEqual(other: Any): Boolean = other.isInstanceOf[WindowCloseTypeInformation]
  override def equals(other: Any): Boolean = canEqual(other)
  override def hashCode(): Int = classOf[WindowCloseTypeInformation].hashCode()
  override def toString: String = "WindowClose(Protobuf)"
}
