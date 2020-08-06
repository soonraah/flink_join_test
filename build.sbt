import Dependencies._

ThisBuild / scalaVersion     := "2.12.12"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "flink-join-test",
    // libraryDependencies += scalaTest % Test
    libraryDependencies ++= Seq(
      "org.apache.flink" %% "flink-streaming-scala" % "1.11.1",
      "org.apache.flink" %% "flink-test-utils" % "1.11.1",
      scalaTest % Test
    ),
    mainClass in (Compile, run) := Some("example.FlinkJoinTest")
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
