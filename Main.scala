package ottla

import akka.stream.scaladsl._
import akka.stream.io.Framing
import akka.util.ByteString

import kafka.api._
import kafka.common.TopicAndPartition
import kafka.message._

import scala.collection.mutable


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

  val kafka = kafkaProtocol join connection

  val correlationId = Iterator.from(0).next _
  val clientId = "foobar"
  val topic = "topic"
  val partition = TopicAndPartition(topic, 0)

  val metadataRequest = new TopicMetadataRequest(
    0, correlationId(), clientId, Seq(topic))

  val offsetsRequest = OffsetRequest(
    Map(partition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)),
    correlationId = correlationId(),
    clientId = clientId)

  val payload = ByteString("The quick brown fox jumps over the lazy dog.")
  val key = ByteString("tag:poetry")

  val data = new Message(
    bytes = payload.toArray[Byte],
    key = key.toArray[Byte],
    codec = NoCompressionCodec,
    payloadOffset = 0,
    payloadSize = payload.size)

  def fetchRequest(offset: Long) = new FetchRequest(
    correlationId(),
    clientId,
    maxWait = 500,
    minBytes = 64 * 1024,
    requestInfo = Map(partition -> PartitionFetchInfo(offset, 64 * 1024)))

  val produceRequest = new ProducerRequest(
    correlationId(),
    clientId = clientId,
    requiredAcks = -1.toShort,
    ackTimeoutMs = 8 * 1000,
    data = mutable.Map(partition -> new ByteBufferMessageSet(data)))

  val message = Vector[RequestOrResponse](
    metadataRequest,
    offsetsRequest,
    produceRequest,
    offsetsRequest,
    fetchRequest(9))

  Source(message)
    .via(kafka)
    .recover(errorHandler)
    .map(KafkaApi.formatResponse)
    .runForeach(println)
}

