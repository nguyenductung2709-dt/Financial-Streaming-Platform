name := "streaming-processor"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.13.18"

val flinkVersion = "2.3.0"

libraryDependencies ++= Seq(
  "org.apache.flink" % "flink-streaming-java" % flinkVersion % "provided",
  "org.apache.flink" % "flink-clients" % flinkVersion % "provided",
  "com.google.protobuf" % "protobuf-java" % "4.33.4"
)

Compile / unmanagedSourceDirectories +=
  (baseDirectory.value / "../../gen/java").getCanonicalFile