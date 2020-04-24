name := "kafka-streams-example-scala"

version := "0.1"

scalaVersion := "2.13.1"

libraryDependencies += "org.apache.kafka" %% "kafka-streams-scala" % "2.5.0"
libraryDependencies += "org.scalatest" % "scalatest_2.13" % "3.1.1" % "test"
libraryDependencies += "org.apache.kafka" % "kafka-streams-test-utils" % "2.5.0" % Test
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.10.3"
