package financial.streaming

import financial.streaming.config.ProcessorConfig

import org.apache.flink.api.connector.source.Boundedness
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import scala.jdk.CollectionConverters._

class StreamingProcessorSuite extends munit.FunSuite {
  test("continuous source defaults and bounded replay source") {
    assertEquals(
      StreamingProcessor.source(ProcessorConfig()).getBoundedness,
      Boundedness.CONTINUOUS_UNBOUNDED
    )
    assertEquals(
      StreamingProcessor.source(ProcessorConfig(bounded = true)).getBoundedness,
      Boundedness.BOUNDED
    )
  }
  private val defaultIdentifiers = Set(
    "trade-events-source-v1",
    "five-minute-closing-prices-v1",
    "window-ema-values-v1",
    "trade-advisories-values-v1",
    "trade-advisories-v1",
    "advisory-redpanda-output-v1",
    "Sink Committer: advisory-redpanda-output-v1",
    "ema-inspection-v1",
    "ema-redpanda-output-v1",
    "Sink Committer: ema-redpanda-output-v1"
  )

  private def identifiers(cfg: ProcessorConfig): Set[String] = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    StreamingProcessor.configure(env, cfg)
    StreamingProcessor.buildPipeline(env, cfg)
    assertEquals(env.getCheckpointConfig.getCheckpointInterval, 30000L)
    env.getStreamGraph.getStreamNodes.asScala.map(_.getTransformationUID).toSet
  }

  test("production pipeline builds without executing and preserves stable operator identifiers") {
    assertEquals(identifiers(ProcessorConfig(sampleLimit = 0)), defaultIdentifiers)
  }

  test("latency sink is added only when the flag is set") {
    val cfg = ProcessorConfig(sampleLimit = 0, latencyTopic = Some("latency-stamps"))
    assertEquals(
      identifiers(cfg),
      defaultIdentifiers +
        "latency-stamps-redpanda-output-v1" +
        "Sink Committer: latency-stamps-redpanda-output-v1"
    )
  }
}
