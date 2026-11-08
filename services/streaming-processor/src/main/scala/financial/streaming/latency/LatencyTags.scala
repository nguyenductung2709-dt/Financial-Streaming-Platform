package financial.streaming.latency

import financial.streaming.model.StageStamp
import financial.streaming.serialization.StageStampTypeInformation
import org.apache.flink.util.OutputTag

/** Side outputs that carry per-stage latency stamps out of the pipeline's operators. */
object LatencyTags {
  val WindowFired: OutputTag[StageStamp] =
    new OutputTag[StageStamp]("window-fired-stamps", new StageStampTypeInformation)
  val EmaComputed: OutputTag[StageStamp] =
    new OutputTag[StageStamp]("ema-computed-stamps", new StageStampTypeInformation)
  val CrossoverEvaluated: OutputTag[StageStamp] =
    new OutputTag[StageStamp]("crossover-evaluated-stamps", new StageStampTypeInformation)
}
