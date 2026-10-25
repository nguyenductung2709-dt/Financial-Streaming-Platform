package financial.streaming.advisory

import financial.streaming.StreamingProcessor
import financial.streaming.model.{CrossoverResult, EmaResult, WindowClose}
import financial.streaming.serialization.EmaResultTypeInformation
import java.math.BigDecimal
import market.v1.Advisory.AdvisoryAction
import market.v1.Events.PriceEvent
import org.apache.flink.configuration.{Configuration, PipelineOptions}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import scala.jdk.CollectionConverters._

class CrossoverPipelineSuite extends munit.FunSuite {
  override val munitTimeout: scala.concurrent.duration.Duration =
    scala.concurrent.duration.Duration(60, "seconds")

  private def ema(symbol: String, start: Long, short: Long, long: Long): EmaResult =
    EmaResult(
      WindowClose(
        PriceEvent.newBuilder().setSymbol(symbol).setLastPriceMicros(39000000L).build(),
        start,
        start + 300000L
      ),
      BigDecimal.valueOf(short),
      BigDecimal.valueOf(long)
    )

  test("production crossover wiring groups by symbol and uses explicit Flink serialization") {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    val config = new Configuration()
    config.set(PipelineOptions.GENERIC_TYPES, java.lang.Boolean.FALSE)
    env.configure(config)
    val firstA = ema("RUI.FR", 0L, 3L, 2L)
    val firstB = ema("RUI.ETR", 0L, 4L, 2L)
    val secondA = ema("RUI.FR", 300000L, 1L, 2L)
    val secondB = ema("RUI.ETR", 300000L, 5L, 2L)
    val input =
      env.fromData(List(firstA, firstB, secondA, secondB).asJava, new EmaResultTypeInformation)
    val iterator = StreamingProcessor.crossoverResults(input).executeAndCollect()
    val results =
      try iterator.asScala.toList
      finally iterator.close()
    assertEquals(results.size, 3)
    assertEquals(
      results.toSet,
      Set(
        CrossoverResult(
          firstA,
          AdvisoryAction.ADVISORY_ACTION_BUY,
          BigDecimal.ZERO,
          BigDecimal.ZERO
        ),
        CrossoverResult(
          firstB,
          AdvisoryAction.ADVISORY_ACTION_BUY,
          BigDecimal.ZERO,
          BigDecimal.ZERO
        ),
        CrossoverResult(
          secondA,
          AdvisoryAction.ADVISORY_ACTION_SELL,
          BigDecimal.valueOf(3L),
          BigDecimal.valueOf(2L)
        )
      )
    )
  }
}
