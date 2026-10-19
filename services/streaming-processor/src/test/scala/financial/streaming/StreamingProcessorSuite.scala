package financial.streaming

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
  test("production pipeline builds without executing and preserves stable operator identifiers") {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val cfg = ProcessorConfig(sampleLimit = 0)
    StreamingProcessor.configure(env, cfg)
    StreamingProcessor.buildPipeline(env, cfg)
    val graph = env.getStreamGraph
    assertEquals(env.getCheckpointConfig.getCheckpointInterval, 30000L)
    val identifiers = graph.getStreamNodes.asScala.map(_.getTransformationUID).toSet
    assertEquals(identifiers, Set("trade-events-source-v1", "decoded-event-inspection-v1"))
  }
}
