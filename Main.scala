package ottla

import akka.stream.scaladsl._
import akka.stream.io.Framing
import akka.util.ByteString

import kafka.api._



object Main extends StreamApp {
  def errorHandler[T] = PartialFunction[Throwable, T] {
    case error => println(error); throw error
  }

  import KafkaApi._
  val kafkaApi = CorrelatedBidiFlow(encodeRequest _, decodeResponse _)
  val framing = Framing.simpleFramingProtocol(Int.MaxValue - 4)
  val kafkaProtocol = kafkaApi atop framing

  val host = scala.sys.process.Process("boot2docker ip").!!.trim
  val connection = Tcp().outgoingConnection(host, 9092)


  val message = new TopicMetadataRequest(42, 9000, "foobar", Seq("topic"))

  Source.single(message)
    .via(kafkaProtocol join connection)
    .recover(errorHandler)
    .runForeach(println)
}

