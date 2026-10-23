package financial.streaming

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException}
import com.google.protobuf.Timestamp
import financial.streaming.model.WindowClose
import market.v1.Events.{PriceEvent, SecurityType}
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot
import org.apache.flink.core.memory.{DataInputViewStreamWrapper, DataOutputViewStreamWrapper}

class WindowCloseSerializerSuite extends munit.FunSuite {
  private val serializer = new WindowCloseSerializer
  private val window = WindowClose(
    PriceEvent
      .newBuilder()
      .setSymbol("RUI.FR")
      .setSecurityType(SecurityType.SECURITY_TYPE_EQUITY)
      .setLastPriceMicros(-1L)
      .setSourceRow(-1L)
      .setTradeTime(Timestamp.newBuilder().setSeconds(1636700699L).setNanos(999999999))
      .build(),
    1636700400000L,
    1636700700000L
  )

  private def encoded(value: WindowClose): Array[Byte] = {
    val bytes = new ByteArrayOutputStream
    serializer.serialize(value, new DataOutputViewStreamWrapper(bytes))
    bytes.toByteArray
  }

  private def input(bytes: Array[Byte]): DataInputViewStreamWrapper =
    new DataInputViewStreamWrapper(new ByteArrayInputStream(bytes))

  test("round trip preserves boundaries, nanoseconds and full unsigned price/source-row values") {
    val result = serializer.deserialize(input(encoded(window)))
    assertEquals(result, window)
    assertEquals(result.symbol, "RUI.FR")
    assertEquals(
      java.lang.Long.toUnsignedString(result.closingTrade.getLastPriceMicros),
      "18446744073709551615"
    )
    assertEquals(
      java.lang.Long.toUnsignedString(result.closingTrade.getSourceRow),
      "18446744073709551615"
    )
  }

  test("sequential windows retain distinct boundaries and closing trades") {
    val next = WindowClose(
      window.closingTrade.toBuilder.setSymbol("OTHER.FR").setLastPriceMicros(28040000L).build(),
      window.windowEndMillis,
      window.windowEndMillis + 300000L
    )
    val bytes = new ByteArrayOutputStream
    val out = new DataOutputViewStreamWrapper(bytes)
    serializer.serialize(window, out)
    serializer.serialize(next, out)
    val in = input(bytes.toByteArray)
    assertEquals(serializer.deserialize(in), window)
    assertEquals(serializer.deserialize(in), next)
    assertEquals(in.read(), -1)
  }

  test("binary copy preserves the complete encoded window") {
    val bytes = new ByteArrayOutputStream
    serializer.copy(input(encoded(window)), new DataOutputViewStreamWrapper(bytes))
    assertEquals(serializer.deserialize(input(bytes.toByteArray)), window)
  }

  test("immutable copy and reuse overloads preserve the original values") {
    val reuse = serializer.createInstance()
    assert(serializer.copy(window) eq window)
    assert(serializer.copy(window, reuse) eq window)
    assertEquals(serializer.deserialize(reuse, input(encoded(window))), window)
    assertEquals(reuse, WindowClose(PriceEvent.getDefaultInstance, 0L, 0L))
  }

  test("persisted serializer metadata reconstructs a usable serializer") {
    val bytes = new ByteArrayOutputStream
    TypeSerializerSnapshot.writeVersionedSnapshot(
      new DataOutputViewStreamWrapper(bytes),
      serializer.snapshotConfiguration()
    )
    val snapshot = TypeSerializerSnapshot.readVersionedSnapshot[WindowClose](
      input(bytes.toByteArray),
      getClass.getClassLoader
    )
    assert(snapshot.isInstanceOf[WindowCloseSerializerSnapshot])
    val restored = snapshot.restoreSerializer()
    assertEquals(restored.deserialize(input(encoded(window))), window)
  }

  test("truncated windows fail instead of producing partial results") {
    val bytes = encoded(window)
    Seq(0, 7, 15, 19, bytes.length - 1).foreach { size =>
      intercept[IOException] {
        serializer.deserialize(input(bytes.take(size)))
      }
    }
  }

  test("nested PriceEvent length limits remain enforced") {
    Seq(-1, PriceEventDecoder.MaxMessageBytes + 1).foreach { length =>
      val bytes = new ByteArrayOutputStream
      val out = new DataOutputViewStreamWrapper(bytes)
      out.writeLong(window.windowStartMillis)
      out.writeLong(window.windowEndMillis)
      out.writeInt(length)
      intercept[IOException] {
        serializer.deserialize(input(bytes.toByteArray))
      }
    }
  }
}
