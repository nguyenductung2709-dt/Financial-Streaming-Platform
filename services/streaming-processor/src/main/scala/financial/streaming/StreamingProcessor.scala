package financial.streaming

import financial.streaming.window.ClosingPriceAggregate
import financial.streaming.source.PriceEventDeserializationSchema
import financial.streaming.serialization.PriceEventTypeInformation
import financial.streaming.config.ProcessorConfig
import financial.streaming.sink.{
  AdvisoryLoggingSink,
  AdvisoryRedpandaSink,
  EmaRedpandaSink,
  EmaLoggingSink
}
import financial.streaming.window.WindowCloseFunction
import financial.streaming.serialization.WindowCloseTypeInformation

import financial.streaming.ema.EmaProcessFunction
import financial.streaming.advisory.CrossoverProcessFunction
import financial.streaming.model.{EmaResult, WindowClose, CrossoverResult}
import financial.streaming.serialization.{EmaResultTypeInformation, CrossoverResultTypeInformation}
import market.v1.Events.PriceEvent
import java.time.Duration
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.{DataStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.kafka.clients.consumer.OffsetResetStrategy

object StreamingProcessor {
  private val OutOfOrdernessAllowance = Duration.ofSeconds(70)

  private[streaming] def priceEventWatermarks: WatermarkStrategy[PriceEvent] =
    WatermarkStrategy
      .forBoundedOutOfOrderness[PriceEvent](OutOfOrdernessAllowance)
      .withTimestampAssigner(new SerializableTimestampAssigner[PriceEvent] {
        override def extractTimestamp(
            event: PriceEvent,
            previousTimestamp: Long
        ): Long = {
          val timestamp = event.getTradeTime

          timestamp.getSeconds * 1000L +
            timestamp.getNanos / 1000000L
        }
      })

  def source(cfg: ProcessorConfig): KafkaSource[PriceEvent] = {
    val offsets = cfg.startingOffsets match {
      case "earliest"  => OffsetsInitializer.earliest()
      case "latest"    => OffsetsInitializer.latest()
      case "committed" => OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)
    }
    val builder = KafkaSource
      .builder[PriceEvent]()
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

  // UTC and fixed CEST (UTC+02) have the same five-minute boundaries.
  private[streaming] def closingPrices(
      events: DataStream[PriceEvent]
  ): SingleOutputStreamOperator[WindowClose] =
    events
      .keyBy(
        new KeySelector[PriceEvent, String] {
          override def getKey(event: PriceEvent): String = event.getSymbol
        },
        Types.STRING
      )
      .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
      .aggregate(
        new ClosingPriceAggregate,
        new WindowCloseFunction,
        new PriceEventTypeInformation,
        new PriceEventTypeInformation,
        new WindowCloseTypeInformation
      )
      .name("five-minute-closing-prices")
      .uid("five-minute-closing-prices-v1")

  private[streaming] def emaValues(
      windows: DataStream[WindowClose]
  ): SingleOutputStreamOperator[EmaResult] =
    windows
      .keyBy(
        new KeySelector[WindowClose, String] {
          override def getKey(window: WindowClose): String = window.symbol
        },
        Types.STRING
      )
      .process(new EmaProcessFunction, new EmaResultTypeInformation)
      .name("window-ema-values")
      .uid("window-ema-values-v1")

  private[streaming] def crossoverResults(
      emaResults: DataStream[EmaResult]
  ): SingleOutputStreamOperator[CrossoverResult] =
    emaResults
      .keyBy(
        new KeySelector[EmaResult, String] {
          override def getKey(result: EmaResult): String = result.window.symbol
        },
        Types.STRING
      )
      .process(new CrossoverProcessFunction, new CrossoverResultTypeInformation)
      .name("trade-advisories-values")
      .uid("trade-advisories-values-v1")

  // Describe the operators without submitting or executing a job.
  def buildPipeline(env: StreamExecutionEnvironment, cfg: ProcessorConfig): Unit = {
    val events = env
      .fromSource(source(cfg), priceEventWatermarks, "trade-events-source")
      .uid("trade-events-source-v1")
    val emaResults = emaValues(closingPrices(events))
    emaResults
      .sinkTo(new EmaLoggingSink(cfg.sampleLimit))
      .name("ema-inspection")
      .uid("ema-inspection-v1")
    emaResults
      .sinkTo(EmaRedpandaSink.create(cfg.brokers, cfg.outputTopic))
      .name("ema-redpanda-output")
      .uid("ema-redpanda-output-v1")

    val tradeAdvisoriesResults = crossoverResults(emaResults)
    tradeAdvisoriesResults
      .sinkTo(new AdvisoryLoggingSink(cfg.sampleLimit))
      .name("trade-advisories-inspection")
      .uid("trade-advisories-v1")
    tradeAdvisoriesResults
      .sinkTo(AdvisoryRedpandaSink.create(cfg.brokers, cfg.advisoryTopic))
      .name("advisory-redpanda-output")
      .uid("advisory-redpanda-output-v1")
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
