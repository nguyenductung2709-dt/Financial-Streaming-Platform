package financial.streaming.config

class ProcessorConfigSuite extends munit.FunSuite {
  test("defaults resume committed offsets and bound sample logging") {
    val cfg = ProcessorConfig.parse(Array.empty)
    assertEquals(cfg.startingOffsets, "committed")
    assertEquals(cfg.sampleLimit, 5)
    assertEquals(cfg.outputTopic, "ema-windows")
    assertEquals(cfg.advisoryTopic, "trade-advisories")
    assert(!cfg.bounded)
  }
  test("defaults leave latency stamps off") {
    assertEquals(ProcessorConfig.parse(Array.empty).latencyTopic, None)
  }
  test("--latency-topic enables the stamp topic") {
    val cfg = ProcessorConfig.parse(Array("--latency-topic", "latency-stamps"))
    assertEquals(cfg.latencyTopic, Some("latency-stamps"))
  }
  test("explicit replay configuration") {
    val cfg = ProcessorConfig.parse(
      Array(
        "--brokers",
        " a:9092, b:9092 ",
        "--topic",
        "prices",
        "--output-topic",
        "ema-results",
        "--advisory-topic",
        "advice",
        "--group-id",
        "replay",
        "--starting-offsets",
        "earliest",
        "--parallelism",
        "2",
        "--checkpoint-interval-ms",
        "1000",
        "--sample-limit",
        "0",
        "--bounded"
      )
    )
    assertEquals(
      cfg,
      ProcessorConfig(
        "a:9092,b:9092",
        "prices",
        "replay",
        "earliest",
        2,
        1000L,
        0,
        true,
        "ema-results",
        "advice"
      )
    )
  }
  test("reject invalid arguments instead of silently using defaults") {
    Seq(
      Array("--unknown"),
      Array("--advisory-topic"),
      Array("--advisory-topic", " "),
      Array("--advisory-topic", ".."),
      Array("--advisory-topic", "contains spaces"),
      Array("--advisory-topic", "a" * 250),
      Array("--advisory-topic", "a", "--advisory-topic", "b"),
      Array("--advisory-topic", "trade-events"),
      Array("--advisory-topic", "ema-windows"),
      Array("--topic", "trade-advisories"),
      Array("--output-topic", "trade-advisories"),
      Array("--advisory-topic", "same", "--topic", "same"),
      Array("--topic", "same", "--advisory-topic", "same"),
      Array("--advisory-topic", "same", "--output-topic", "same"),
      Array("--output-topic", "same", "--advisory-topic", "same"),
      Array("--output-topic"),
      Array("--output-topic", " "),
      Array("--output-topic", ".."),
      Array("--output-topic", "contains spaces"),
      Array("--output-topic", "a" * 250),
      Array("--output-topic", "a", "--output-topic", "b"),
      Array("--output-topic", "trade-events"),
      Array("--topic", "ema-windows"),
      Array("--topic", "same", "--output-topic", "same"),
      Array("--output-topic", "same", "--topic", "same"),
      Array("--topic"),
      Array("--topic", ""),
      Array("--topic", ".."),
      Array("--topic", "contains spaces"),
      Array("--brokers", "host:9092,"),
      Array("--parallelism", "0"),
      Array("--parallelism", "2147483648"),
      Array("--checkpoint-interval-ms", "999"),
      Array("--sample-limit", "-1"),
      Array("--starting-offsets", "random"),
      Array("--topic", "a", "--topic", "b"),
      Array("--topic", "--bounded"),
      Array("--latency-topic"),
      Array("--latency-topic", " "),
      Array("--latency-topic", ".."),
      Array("--latency-topic", "contains spaces"),
      Array("--latency-topic", "a" * 250),
      Array("--latency-topic", "a", "--latency-topic", "b"),
      Array("--latency-topic", "trade-events"),
      Array("--latency-topic", "ema-windows"),
      Array("--latency-topic", "trade-advisories"),
      Array("--latency-topic", "same", "--topic", "same"),
      Array("--topic", "same", "--latency-topic", "same"),
      Array("--latency-topic", "same", "--output-topic", "same"),
      Array("--latency-topic", "same", "--advisory-topic", "same"),
      Array("positional")
    )
      .foreach(args => intercept[IllegalArgumentException](ProcessorConfig.parse(args)))
  }
}
