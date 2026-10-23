package financial.streaming.serialization

import financial.streaming.model.{EmaResult, WindowClose}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, EOFException}
import java.math.BigDecimal
import market.v1.Events.PriceEvent
import org.apache.flink.api.common.serialization.SerializerConfigImpl
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot
import org.apache.flink.core.memory.{DataInputViewStreamWrapper, DataOutputViewStreamWrapper}

class EmaResultSerializerSuite extends munit.FunSuite {
  private val result = EmaResult(
    WindowClose(
      PriceEvent
        .newBuilder()
        .setSymbol("IUSAC.FR")
        .setLastPriceMicros(-1L)
        .setSourceRow(-1L)
        .build(),
      1636693200000L,
      1636693500000L
    ),
    new BigDecimal("21.44666666666666666666666666666667"),
    new BigDecimal("8.281386138613861386138613861386139")
  )

  private def input(bytes: Array[Byte]): DataInputViewStreamWrapper =
    new DataInputViewStreamWrapper(new ByteArrayInputStream(bytes))
  private def encode(): Array[Byte] = {
    val bytes = new ByteArrayOutputStream
    new EmaResultSerializer().serialize(result, new DataOutputViewStreamWrapper(bytes))
    bytes.toByteArray
  }

  test("explicit result serializer and binary copy preserve all decimal and trade fields") {
    val serializer = new EmaResultTypeInformation().createSerializer(new SerializerConfigImpl)
    assert(serializer.isInstanceOf[EmaResultSerializer])
    assertEquals(serializer.deserialize(input(encode())), result)
    val copied = new ByteArrayOutputStream
    serializer.copy(input(encode()), new DataOutputViewStreamWrapper(copied))
    assertEquals(serializer.deserialize(input(copied.toByteArray)), result)
  }

  test("persisted serializer metadata restores a working serializer") {
    val bytes = new ByteArrayOutputStream
    TypeSerializerSnapshot.writeVersionedSnapshot(
      new DataOutputViewStreamWrapper(bytes),
      new EmaResultSerializer().snapshotConfiguration()
    )
    val snapshot = TypeSerializerSnapshot
      .readVersionedSnapshot[EmaResult](input(bytes.toByteArray), getClass.getClassLoader)
    assertEquals(snapshot.restoreSerializer().deserialize(input(encode())), result)
  }

  test("truncated results fail instead of producing incomplete EMA values") {
    intercept[EOFException](new EmaResultSerializer().deserialize(input(encode().dropRight(1))))
  }
}
