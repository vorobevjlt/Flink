name := "flink"

version := "0.1"

scalaVersion := "2.13.10"

val flinkVersion = "1.17.0"
val logbackVersion = "1.2.12"

val flinkDependencies = Seq(
  "org.apache.flink" % "flink-streaming-java" % flinkVersion,
  "org.apache.flink" % "flink-clients" % flinkVersion,
  "org.apache.flink" % "flink-test-utils" % flinkVersion
)

val logbackDependencies = Seq(
  "ch.qos.logback" % "logback-core" % logbackVersion,
  "ch.qos.logback" % "logback-classic" % logbackVersion
)

libraryDependencies ++= flinkDependencies ++ logbackDependencies
