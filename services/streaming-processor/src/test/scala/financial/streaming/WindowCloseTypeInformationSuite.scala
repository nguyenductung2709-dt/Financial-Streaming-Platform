package financial.streaming

import financial.streaming.model.WindowClose
import market.v1.Events.PriceEvent
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SerializerConfigImpl
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import java.time.Duration

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

  test("Flink infers the registered result type when combining aggregate and window function") {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val events = env.fromSource(
      StreamingProcessor.source(ProcessorConfig()),
      WatermarkStrategy.noWatermarks[PriceEvent](),
      "type-registration-test"
    )
    val windows = events
      .keyBy(
        new KeySelector[PriceEvent, String] {
          override def getKey(event: PriceEvent): String = event.getSymbol
        },
        Types.STRING
      )
      .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
      .aggregate(new ClosingPriceAggregate, new WindowCloseFunction)

    assert(windows.getType.isInstanceOf[WindowCloseTypeInformation])
    assert(
      windows.getType.createSerializer(new SerializerConfigImpl).isInstanceOf[WindowCloseSerializer]
    )
  }
}
