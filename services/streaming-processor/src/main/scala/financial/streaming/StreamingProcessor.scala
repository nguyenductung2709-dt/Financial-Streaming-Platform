package financial.streaming

import market.v1.Events.PriceEvent
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.kafka.clients.consumer.OffsetResetStrategy

object StreamingProcessor {
  def source(cfg: ProcessorConfig): KafkaSource[PriceEvent] = {
    val offsets = cfg.startingOffsets match {
      case "earliest" => OffsetsInitializer.earliest()
      case "latest" => OffsetsInitializer.latest()
      case "committed" => OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)
    }
    val builder = KafkaSource.builder[PriceEvent]()
      .setBootstrapServers(cfg.brokers)
      .setTopics(cfg.topic)
      .setGroupId(cfg.groupId)
      .setStartingOffsets(offsets)
      .setDeserializer(new PriceEventDeserializationSchema)
      .setProperty("enable.auto.commit", "false")
      .setProperty("commit.offsets.on.checkpoint", "true")
      .setProperty("allow.auto.create.topics", "false")
      .setProperty("isolation.level", "read_committed")
    if (cfg.bounded) builder.setBounded(OffsetsInitializer.latest())
    builder.build()
  }

  def configure(env: StreamExecutionEnvironment, cfg: ProcessorConfig): Unit = {
    env.setParallelism(cfg.parallelism)
    val recovery = new Configuration()
    recovery.setString("restart-strategy.type", "fixed-delay")
    recovery.setString("restart-strategy.fixed-delay.attempts", "3")
    recovery.setString("restart-strategy.fixed-delay.delay", "10 s")
    env.configure(recovery)
    env.enableCheckpointing(cfg.checkpointIntervalMs)
    val checkpoints = env.getCheckpointConfig
    checkpoints.setCheckpointTimeout(120000L)
    checkpoints.setMinPauseBetweenCheckpoints(1000L)
    checkpoints.setMaxConcurrentCheckpoints(1)
  }

  // Describe the operators without submitting or executing a job.
  def buildPipeline(env: StreamExecutionEnvironment, cfg: ProcessorConfig): Unit = {
    env.fromSource(source(cfg), WatermarkStrategy.noWatermarks[PriceEvent](), "trade-events-source")
      .uid("trade-events-source-v1")
      .sinkTo(new SampleLoggingSink(cfg.sampleLimit))
      .name("decoded-event-inspection")
      .uid("decoded-event-inspection-v1")
  }

  def main(args: Array[String]): Unit = {
    if (args.sameElements(Array("--help"))) {
      println(ProcessorConfig.usage)
      return
    }
    val cfg = ProcessorConfig.parse(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    configure(env, cfg)
    buildPipeline(env, cfg)
    env.execute("market-price-event-consumer")
  }
}
