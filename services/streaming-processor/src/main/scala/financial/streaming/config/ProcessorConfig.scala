package financial.streaming.config

final case class ProcessorConfig(
    brokers: String = "redpanda-0:9092",
    topic: String = "trade-events",
    groupId: String = "streaming-processor",
    startingOffsets: String = "committed",
    parallelism: Int = 1,
    checkpointIntervalMs: Long = 30000L,
    sampleLimit: Int = 5,
    bounded: Boolean = false
)

object ProcessorConfig {
  val usage: String = """Usage: streaming-processor [options]
    |  --brokers <addresses>           Kafka bootstrap servers (default redpanda-0:9092)
    |  --topic <name>                  Input topic (default trade-events)
    |  --group-id <name>               Consumer group (default streaming-processor)
    |  --starting-offsets <mode>       committed, earliest, latest (default committed)
    |  --parallelism <number>          Operator parallelism (default 1)
    |  --checkpoint-interval-ms <ms>   Checkpoint interval, at least 1000 (default 30000)
    |  --sample-limit <number>         Completed windows logged per sink subtask/attempt (default 5; 0 disables)
    |  --bounded                      Stop at the offsets present when the job starts
    |  --help                         Show this help
    |""".stripMargin

  def parse(args: Array[String]): ProcessorConfig = {
    var cfg = ProcessorConfig()
    var i = 0
    val seen = scala.collection.mutable.Set.empty[String]
    while (i < args.length) {
      val option = args(i)
      require(seen.add(option), s"duplicate option: $option")
      if (option == "--bounded") cfg = cfg.copy(bounded = true)
      else {
        require(
          Set(
            "--brokers",
            "--topic",
            "--group-id",
            "--starting-offsets",
            "--parallelism",
            "--checkpoint-interval-ms",
            "--sample-limit"
          ).contains(option),
          s"unknown option: $option"
        )
        i += 1
        require(i < args.length && !args(i).startsWith("--"), s"missing value for $option")
        val value = args(i).trim
        require(value.nonEmpty, s"$option must not be empty")
        def intValue: Int = value.toIntOption.getOrElse(
          throw new IllegalArgumentException(s"$option must be an integer")
        )
        cfg = option match {
          case "--brokers" =>
            val addresses = value.split(",", -1).map(_.trim)
            require(addresses.forall(_.nonEmpty), "brokers must not contain empty addresses")
            cfg.copy(brokers = addresses.mkString(","))
          case "--topic" =>
            require(
              value.matches("[a-zA-Z0-9._-]{1,249}") && value != "." && value != "..",
              "invalid Kafka topic name"
            )
            cfg.copy(topic = value)
          case "--group-id"         => cfg.copy(groupId = value)
          case "--starting-offsets" =>
            require(
              Set("committed", "earliest", "latest").contains(value),
              "starting-offsets must be committed, earliest, or latest"
            )
            cfg.copy(startingOffsets = value)
          case "--parallelism" =>
            require(intValue > 0, "parallelism must be positive")
            cfg.copy(parallelism = intValue)
          case "--checkpoint-interval-ms" =>
            val ms = value.toLongOption.getOrElse(
              throw new IllegalArgumentException("checkpoint interval must be an integer")
            )
            require(ms >= 1000, "checkpoint interval must be at least 1000 ms")
            cfg.copy(checkpointIntervalMs = ms)
          case "--sample-limit" =>
            require(intValue >= 0, "sample limit must be nonnegative")
            cfg.copy(sampleLimit = intValue)
        }
      }
      i += 1
    }
    cfg
  }
}
