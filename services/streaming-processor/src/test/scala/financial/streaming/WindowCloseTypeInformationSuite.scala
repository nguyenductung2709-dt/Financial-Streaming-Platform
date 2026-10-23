package financial.streaming

import financial.streaming.model.WindowClose
import market.v1.Events.PriceEvent
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SerializerConfigImpl
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

class WindowCloseTypeInformationSuite extends munit.FunSuite {
  test("type information selects the explicit window serializer") {
    val info = new WindowCloseTypeInformation
    assertEquals(info.getTypeClass, classOf[WindowClose])
    assert(info.createSerializer(new SerializerConfigImpl).isInstanceOf[WindowCloseSerializer])
    assert(!info.isKeyType)
  }

  test("equivalent window types have consistent equality and hashes") {
    val first = new WindowCloseTypeInformation
    val second = new WindowCloseTypeInformation
    assertEquals(first, second)
    assertEquals(first.hashCode(), second.hashCode())
    assert(!first.equals(new PriceEventTypeInformation))
    assert(!first.equals(null))
  }

  test("production windows select the explicit result serializer") {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val events = env.fromSource(
      StreamingProcessor.source(ProcessorConfig()),
      WatermarkStrategy.noWatermarks[PriceEvent](),
      "type-registration-test"
    )
    val windows = StreamingProcessor.closingPrices(events)

    assert(windows.getType.isInstanceOf[WindowCloseTypeInformation])
    assert(
      windows.getType.createSerializer(new SerializerConfigImpl).isInstanceOf[WindowCloseSerializer]
    )
  }
}
