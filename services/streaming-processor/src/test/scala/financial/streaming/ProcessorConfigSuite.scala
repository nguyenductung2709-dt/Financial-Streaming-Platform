package financial.streaming

class ProcessorConfigSuite extends munit.FunSuite {
  test("defaults resume committed offsets and bound sample logging") {
    val cfg = ProcessorConfig.parse(Array.empty)
    assertEquals(cfg.startingOffsets, "committed")
    assertEquals(cfg.sampleLimit, 5)
    assert(!cfg.bounded)
  }
  test("explicit replay configuration") {
    val cfg = ProcessorConfig.parse(Array("--brokers", " a:9092, b:9092 ", "--topic", "prices",
      "--group-id", "replay", "--starting-offsets", "earliest", "--parallelism", "2",
      "--checkpoint-interval-ms", "1000", "--sample-limit", "0", "--bounded"))
    assertEquals(cfg, ProcessorConfig("a:9092,b:9092", "prices", "replay", "earliest", 2, 1000L, 0, true))
  }
  test("reject invalid arguments instead of silently using defaults") {
    Seq(Array("--unknown"), Array("--topic"), Array("--topic", ""), Array("--topic", ".."),
      Array("--topic", "contains spaces"), Array("--brokers", "host:9092,"),
      Array("--parallelism", "0"), Array("--parallelism", "2147483648"),
      Array("--checkpoint-interval-ms", "999"), Array("--sample-limit", "-1"),
      Array("--starting-offsets", "random"), Array("--topic", "a", "--topic", "b"),
      Array("--topic", "--bounded"), Array("positional"))
      .foreach(args => intercept[IllegalArgumentException](ProcessorConfig.parse(args)))
  }
}
