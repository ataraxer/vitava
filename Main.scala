package ottla

import akka.stream._
import akka.stream.scaladsl._
import akka.stream.io.Framing
import akka.util.ByteString

import kafka.api._


object Main extends StreamApp {
  val correlator = BidiFlow() { implicit builder =>
    import FlowGraph.Implicits._

    val bcast = builder add Broadcast[RequestOrResponse](2)
    val zipKey = builder add ZipWith(KafkaApi.zipWithKey _)

    bcast ~> Flow[RequestOrResponse].map( _.requestId.get ) ~> zipKey.in0

    BidiShape(bcast.in, bcast.out(1), zipKey.in1, zipKey.out)
  }

  val kafkaApi = BidiFlow(KafkaApi.encodeRequest _, KafkaApi.decodeResponse _)
  val framing = Framing.simpleFramingProtocol(Int.MaxValue - 4)
  val kafkaProtocol = correlator atop kafkaApi atop framing

  val host = scala.sys.process.Process("boot2docker ip").!!.trim
  val connection = Tcp().outgoingConnection(host, 9092)

  def errorHandler[T] = PartialFunction[Throwable, T] {
    case error =>
      println(error)
      throw error
  }

  val message = new TopicMetadataRequest(42, 9000, "foobar", Seq("topic"))

  Source.single(message)
    .via(kafkaProtocol join connection)
    .recover(errorHandler)
    .runForeach(println)
}

