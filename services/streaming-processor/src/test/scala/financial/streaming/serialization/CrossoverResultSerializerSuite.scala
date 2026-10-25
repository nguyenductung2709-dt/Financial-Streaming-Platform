package financial.streaming.serialization

import com.google.protobuf.Timestamp
import financial.streaming.model.{CrossoverResult, EmaResult, WindowClose}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException}
import java.math.BigDecimal
import java.nio.ByteBuffer
import market.v1.Advisory.AdvisoryAction
import market.v1.Events.PriceEvent
import org.apache.flink.api.common.serialization.SerializerConfigImpl
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot
import org.apache.flink.core.memory.{DataInputViewStreamWrapper, DataOutputViewStreamWrapper}

class CrossoverResultSerializerSuite extends munit.FunSuite {
  private val serializer = new CrossoverResultSerializer
  private val result = CrossoverResult(
    EmaResult(
      WindowClose(
        PriceEvent
          .newBuilder()
          .setSymbol("IUSAC.FR")
          .setLastPriceMicros(-1L)
          .setSourceRow(-1L)
          .setTradeTime(Timestamp.newBuilder().setSeconds(1636693499L).setNanos(999999999))
          .build(),
        1636693200000L,
        1636693500000L
      ),
      new BigDecimal("21.44666666666666666666666666666667"),
      new BigDecimal("8.281386138613861386138613861386139")
    ),
    AdvisoryAction.ADVISORY_ACTION_BUY,
    new BigDecimal("7.000000000000000000000000000000001"),
    new BigDecimal("8.00")
  )

  private def input(bytes: Array[Byte]): DataInputViewStreamWrapper =
    new DataInputViewStreamWrapper(new ByteArrayInputStream(bytes))

  private def encode(value: CrossoverResult): Array[Byte] = {
    val bytes = new ByteArrayOutputStream
    serializer.serialize(value, new DataOutputViewStreamWrapper(bytes))
    bytes.toByteArray
  }

  test("explicit type information selects the serializer and preserves all fields") {
    val selected = new CrossoverResultTypeInformation().createSerializer(new SerializerConfigImpl)
    assert(selected.isInstanceOf[CrossoverResultSerializer])
    Seq(
      AdvisoryAction.ADVISORY_ACTION_BUY,
      AdvisoryAction.ADVISORY_ACTION_SELL,
      AdvisoryAction.ADVISORY_ACTION_UNSPECIFIED
    ).foreach { action =>
      val value = result.copy(action = action)
      assertEquals(selected.deserialize(input(encode(value))), value)
    }
  }

  test("binary copy preserves the complete encoded result") {
    val bytes = new ByteArrayOutputStream
    serializer.copy(input(encode(result)), new DataOutputViewStreamWrapper(bytes))
    assertEquals(bytes.toByteArray.toSeq, encode(result).toSeq)
    assertEquals(serializer.deserialize(input(bytes.toByteArray)), result)
  }

  test("sequential results retain their own action, symbol and previous EMA values") {
    val next = result.copy(
      ema = result.ema.copy(window =
        result.ema.window.copy(
          closingTrade = result.ema.window.closingTrade.toBuilder.setSymbol("OTHER.ETR").build()
        )
      ),
      action = AdvisoryAction.ADVISORY_ACTION_SELL,
      previousEma38 = BigDecimal.ZERO
    )
    val in = input(encode(result) ++ encode(next))
    assertEquals(serializer.deserialize(in), result)
    assertEquals(serializer.deserialize(in), next)
    assertEquals(in.read(), -1)
  }

  test("immutable copy, duplicate and reuse overloads preserve values") {
    val reuse = serializer.createInstance()
    assert(serializer.copy(result) eq result)
    assert(serializer.copy(result, reuse) eq result)
    assert(serializer.duplicate() eq serializer)
    assertEquals(serializer.deserialize(reuse, input(encode(result))), result)
    assertEquals(serializer.deserialize(input(encode(reuse))), reuse)
  }

  test("persisted serializer metadata restores a working serializer") {
    val bytes = new ByteArrayOutputStream
    TypeSerializerSnapshot.writeVersionedSnapshot(
      new DataOutputViewStreamWrapper(bytes),
      serializer.snapshotConfiguration()
    )
    val snapshot = TypeSerializerSnapshot.readVersionedSnapshot[CrossoverResult](
      input(bytes.toByteArray),
      getClass.getClassLoader
    )
    assert(snapshot.isInstanceOf[CrossoverResultSerializerSnapshot])
    assertEquals(snapshot.restoreSerializer().deserialize(input(encode(result))), result)
  }

  test("truncated results fail instead of producing incomplete advisories") {
    val bytes = encode(result)
    Seq(0, 7, bytes.length / 2, bytes.length - 1).foreach { size =>
      intercept[IOException](serializer.deserialize(input(bytes.take(size))))
    }
  }

  test("unknown action numbers fail instead of producing a null action") {
    val bytes = new ByteArrayOutputStream
    new EmaResultSerializer().serialize(result.ema, new DataOutputViewStreamWrapper(bytes))
    val corrupted = encode(result)
    ByteBuffer.wrap(corrupted).putInt(bytes.size(), 99)
    intercept[IOException](serializer.deserialize(input(corrupted)))
  }
}
