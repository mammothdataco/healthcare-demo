
name := "Distributed Infection Keyword Checking using NLP and Spark"
version := "0.1.0"
organization := "com.mammothdata"
scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.4.1" % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.4.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.4.1" % "provided",
  "net.databinder.dispatch" %% "dispatch-core" % "0.11.2",
  "org.json4s" %% "json4s-native" % "3.2.10",
  "net.debasishg" %% "redisclient" % "3.0",
  "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test"
)

jarName in assembly := "infection.jar"


assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}