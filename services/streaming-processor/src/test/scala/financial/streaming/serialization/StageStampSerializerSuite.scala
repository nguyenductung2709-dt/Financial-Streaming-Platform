package financial.streaming.serialization

import financial.streaming.model.StageStamp
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, EOFException}
import market.v1.Latency.Stage
import org.apache.flink.api.common.serialization.SerializerConfigImpl
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot
import org.apache.flink.core.memory.{DataInputViewStreamWrapper, DataOutputViewStreamWrapper}

class StageStampSerializerSuite extends munit.FunSuite {
  private val stamps = List(
    StageStamp("IUSAC.FR", 1636693500000L, Stage.STAGE_WINDOW_FIRED, 1636693501234L, false),
    StageStamp("ÜNICODE.FR", 1636693500000L, Stage.STAGE_EMA_COMPUTED, 1636693501240L, false),
    StageStamp("RUI.ETR", 1636693800000L, Stage.STAGE_CROSSOVER_EVALUATED, 1636693801001L, true),
    StageStamp("RUI.ETR", 1636693800000L, Stage.STAGE_CROSSOVER_EVALUATED, 1636693801002L, false)
  )

  private def input(bytes: Array[Byte]): DataInputViewStreamWrapper =
    new DataInputViewStreamWrapper(new ByteArrayInputStream(bytes))
  private def encode(stamp: StageStamp): Array[Byte] = {
    val bytes = new ByteArrayOutputStream
    new StageStampSerializer().serialize(stamp, new DataOutputViewStreamWrapper(bytes))
    bytes.toByteArray
  }

  test("explicit serializer and binary copy preserve every field") {
    val serializer = new StageStampTypeInformation().createSerializer(new SerializerConfigImpl)
    assert(serializer.isInstanceOf[StageStampSerializer])
    stamps.foreach { stamp =>
      assertEquals(serializer.deserialize(input(encode(stamp))), stamp)
      val copied = new ByteArrayOutputStream
      serializer.copy(input(encode(stamp)), new DataOutputViewStreamWrapper(copied))
      assertEquals(serializer.deserialize(input(copied.toByteArray)), stamp)
    }
  }

  test("persisted serializer metadata restores a working serializer") {
    val bytes = new ByteArrayOutputStream
    TypeSerializerSnapshot.writeVersionedSnapshot(
      new DataOutputViewStreamWrapper(bytes),
      new StageStampSerializer().snapshotConfiguration()
    )
    val snapshot = TypeSerializerSnapshot
      .readVersionedSnapshot[StageStamp](input(bytes.toByteArray), getClass.getClassLoader)
    assertEquals(snapshot.restoreSerializer().deserialize(input(encode(stamps.head))), stamps.head)
  }

  test("truncated stamps fail instead of producing partial values") {
    intercept[EOFException](
      new StageStampSerializer().deserialize(input(encode(stamps.head).dropRight(1)))
    )
  }
}
