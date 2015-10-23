package ottla

import akka.actor.ActorSystem
import akka.stream.io.Framing
import akka.stream.scaladsl._


object Kafka {
  import KafkaApi._

  val protocol = {
    val kafkaApi = CorrelatedBidiFlow(encodeRequest _, decodeResponse _)
    val framing = Framing.simpleFramingProtocol(Int.MaxValue - 4)
    kafkaApi atop framing
  }

  def outgoingConnection(host: String, port: Int)(implicit system: ActorSystem) = {
    val connection = Tcp().outgoingConnection(host, port)
    Kafka.protocol join connection
  }
}

