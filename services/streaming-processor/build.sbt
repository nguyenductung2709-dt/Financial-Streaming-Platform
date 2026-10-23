name := "streaming-processor"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.13.18"

val flinkVersion = "2.2.1"

libraryDependencies ++= Seq(
  "org.apache.flink" % "flink-streaming-java" % flinkVersion % "provided",
  "org.apache.flink" % "flink-clients" % flinkVersion % "provided",
  "org.apache.flink" % "flink-connector-base" % flinkVersion,
  "org.apache.flink" % "flink-connector-kafka" % "5.0.0-2.2",
  "com.google.protobuf" % "protobuf-java" % "4.33.4",
  "org.slf4j" % "slf4j-api" % "2.0.17" % "provided",
  // Test-only harness for keyed managed state and checkpoint restoration.
  "org.apache.flink" % "flink-test-utils" % flinkVersion % Test,
  "org.apache.flink" % "flink-runtime" % flinkVersion % Test classifier "tests",
  "org.scalameta" %% "munit" % "1.0.4" % Test
)

Compile / unmanagedSourceDirectories +=
  (baseDirectory.value / "../../gen/java").getCanonicalFile

Compile / javacOptions ++= Seq("--release", "17")
scalacOptions ++= Seq(
  "-release:17",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint:_",
  "-Wunused:imports",
  "-Werror"
)
// Local Flink runtime tests need a single application classpath, outside sbt classloaders.
Test / fork := true

Compile / mainClass := Some("financial.streaming.StreamingProcessor")
assembly / mainClass := (Compile / mainClass).value
assembly / assemblyJarName := "streaming-processor.jar"
// Isolate application runtimes from Scala 2.12 and Protobuf in cluster libraries.
assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("com.google.protobuf.**" -> "financial.streaming.shaded.protobuf.@1").inAll,
  ShadeRule.rename("scala.**" -> "financial.streaming.shaded.scala.@1").inAll
)
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _ @_*) => MergeStrategy.filterDistinctLines
  case PathList("META-INF", xs @ _*)
      if xs.lastOption.exists(n => n.endsWith(".SF") || n.endsWith(".RSA") || n.endsWith(".DSA")) =>
    MergeStrategy.discard
  case "module-info.class"                                      => MergeStrategy.discard
  case PathList("META-INF", "versions", _, "module-info.class") => MergeStrategy.discard
  case x => (assembly / assemblyMergeStrategy).value(x)
}

// Check the actual deployment archive: unit tests run before relocation.
lazy val verifyAssembly = taskKey[File]("Verify deployment runtime isolation")
verifyAssembly := {
  val artifact = assembly.value
  val archive = new java.util.jar.JarFile(artifact)
  try {
    Seq("scala/Predef$.class", "com/google/protobuf/Message.class").foreach { entry =>
      require(archive.getJarEntry(entry) == null, s"Unrelocated runtime in assembly: $entry")
    }
    Seq(
      "financial/streaming/shaded/scala/Predef$.class",
      "financial/streaming/shaded/protobuf/Message.class",
      "org/apache/flink/connector/kafka/source/KafkaSource.class"
    ).foreach { entry =>
      require(archive.getJarEntry(entry) != null, s"Missing deployment dependency: $entry")
    }
  } finally archive.close()
  streams.value.log.info("Verified deployment runtime isolation")
  artifact
}
