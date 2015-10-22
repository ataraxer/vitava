name := "ottla"

version := "0.1.0"

organization := "com.ataraxer"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.3.12",
  "com.typesafe.akka" %% "akka-stream-experimental" % "1.0",
  "com.typesafe.akka" %% "akka-http-core-experimental" % "1.0",
  "com.typesafe.akka" %% "akka-http-experimental" % "1.0",
  "io.spray" %% "spray-can" % "1.3.1",
  "io.spray" %% "spray-http" % "1.3.1",
  "io.spray" %% "spray-client" % "1.3.1",
  "io.spray" %% "spray-routing" % "1.3.1",
  "org.apache.kafka" %% "kafka" % "0.8.2.2",
  "org.json4s" %% "json4s-native" % "3.2.10",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "com.typesafe" % "config" % "1.2.1",
  "org.scodec" %% "scodec-core" % "1.8.0",
  "org.scodec" %% "scodec-bits" % "1.0.10",
  "com.chuusai" %% "shapeless" % "2.2.5")

initialCommands := {
  """
  import akka.actor._
  import akka.stream._
  import akka.stream.scaladsl._
  import akka.stream.io._
  import akka.util.ByteString
  import kafka.api._
  import kafka.common._
  import scala.concurrent.duration._

  implicit val system = ActorSystem("akka-streams")
  implicit val flowBuilder = ActorMaterializer()
  """
}

cleanupCommands := {
  """
  system.shutdown()
  """
}

Revolver.settings

