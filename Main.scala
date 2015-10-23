package ottla

import akka.stream.scaladsl._

import kafka.api._
import kafka.common.TopicAndPartition


object Main extends StreamApp {
  def errorHandler[T] = PartialFunction[Throwable, T] {
    case error => println(error); throw error
  }

  val host = scala.sys.process.Process("boot2docker ip").!!.trim
  val kafka = Kafka(host).outgoingConnection()

  val correlationId = Iterator.from(0).next _
  val clientId = "foobar"
  val topic = "topic"
  val partition = TopicAndPartition(topic, 0)

  val kafkaProducer = Kafka(host).producer(clientId, topic)

  val message = Kafka.Message(
    key = "tag:poetry",
    data = "The quick brown fox jumps over the lazy dog.")

  Source.single(message)
    .via(kafkaProducer)
    .map( offset => f"Ack: $offset")
    .runForeach(println)

  val kafkaConsumer = Kafka(host).consumer(clientId, topic)

  Source.single(0L)
    .via(kafkaConsumer)
    .map( _.data.utf8String )
    .runForeach(println)

  val metadataRequest = new TopicMetadataRequest(
    0, correlationId(), clientId, Seq(topic))

  val offsetsRequest = OffsetRequest(
    Map(partition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)),
    correlationId = correlationId(),
    clientId = clientId)

  val messages = Vector[RequestOrResponse](
    metadataRequest,
    offsetsRequest)

  Source(messages)
    .via(kafka)
    .recover(errorHandler)
    .map(KafkaApi.formatResponse)
    .runForeach(println)
}

