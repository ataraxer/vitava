package ottla

import akka.stream._
import akka.stream.scaladsl._
import akka.stream.io.Framing
import akka.util.ByteString

import kafka.api._


object Main extends StreamApp {
  val codec = BidiFlow() { implicit builder =>
    import FlowGraph.Implicits._
    import KafkaAPI._

    val requestFlow = Flow[RequestOrResponse]

    val getKey = builder add requestFlow.map( _.requestId.get )
    val encode = builder add requestFlow.map(encodeRequest)
    val decode = builder add Flow[ByteString].map(decodeResponse)

    val bcast = builder add Broadcast[RequestOrResponse](2)
    val zipKey = builder add ZipWith(zipWithKey _)

    bcast ~> encode
    bcast ~> getKey ~> zipKey.in0
             decode <~ zipKey.out

    BidiShape(bcast.in, encode.outlet, zipKey.in1, decode.outlet)
  }

  val framing = Framing.simpleFramingProtocol(Int.MaxValue - 4)
  val kafkaProtocol = codec atop framing

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

